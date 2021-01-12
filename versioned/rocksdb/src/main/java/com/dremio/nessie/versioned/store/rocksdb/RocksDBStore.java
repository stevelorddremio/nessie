/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.versioned.store.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Status;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class RocksDBStore implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStore.class);
  private static final long OPEN_SLEEP_MILLIS = 100L;
  private static final List<byte[]> COLUMN_FAMILIES;
  private static final ValueSerDe VALUE_SERDE = new ValueSerDe();

  static {
    RocksDB.loadLibrary();
    COLUMN_FAMILIES = Stream.concat(
      Stream.of(RocksDB.DEFAULT_COLUMN_FAMILY),
      Arrays.stream(ValueType.values()).map(v -> v.name().getBytes(UTF_8))).collect(ImmutableList.toImmutableList());
  }

  private final String dbDirectory;
  RocksDB rocksDB;
  private Map<ValueType, ColumnFamilyHandle> valueTypeToColumnFamily;

  /**
   * Creates a store ready for connection to RocksDB.
   * @param dbDirectory the directory of the Rocks database.
   */
  public RocksDBStore(String dbDirectory) {
    this.dbDirectory = dbDirectory;
  }

  @Override
  public void start() {
    final String dbPath = verifyPath();
    final List<ColumnFamilyDescriptor> columnFamilies = getColumnFamilies(dbPath);

    // TODO: currently an infinite loop, need to configure
    while (true) {
      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
        // TODO: Consider setting WAL limits.
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilies, columnFamilyHandles);
        valueTypeToColumnFamily = IntStream.rangeClosed(1, ValueType.values().length).boxed().collect(
            ImmutableMap.toImmutableMap(k -> ValueType.values()[k - 1], columnFamilyHandles::get));
        break;
      } catch (RocksDBException e) {
        if (e.getStatus().getCode() != Status.Code.IOError || !e.getStatus().getState().contains("While lock")) {
          throw new RuntimeException(e);
        }

        LOGGER.info("Lock file to RocksDB is currently held by another process. Will wait until lock is freed.");
      }

      // Wait a bit before the next attempt.
      try {
        TimeUnit.MILLISECONDS.sleep(OPEN_SLEEP_MILLIS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while opening Nessie RocksDB.", e);
      }
    }
  }

  @Override
  public void close() {
    if (null != rocksDB) {
      try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
        valueTypeToColumnFamily.values().forEach(cf -> {
          try {
            rocksDB.flush(options, cf);
          } catch (RocksDBException e) {
            LOGGER.error("Error flushing column family while closing Nessie RocksDB store.", e);
          }
          cf.close();
        });
      }

      rocksDB.close();
      rocksDB = null;
    }
  }

  @Override
  public void load(LoadStep loadstep) throws NotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final List<LoadOp<?>> loadOps = step.getOps().collect(Collectors.toList());
      if (loadOps.isEmpty()) {
        continue;
      }

      final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(loadOps.size());
      final List<byte[]> keys = new ArrayList<>(loadOps.size());
      loadOps.forEach(op -> {
        columnFamilies.add(getColumnFamilyHandle(op.getValueType()));
        keys.add(op.getId().toBytes());
      });

      final List<byte[]> reads;
      try {
        reads = rocksDB.multiGetAsList(columnFamilies, keys);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }

      if (reads.size() != loadOps.size()) {
        throw new NotFoundException(String.format("[%d] object(s) missing in load.", loadOps.size() - reads.size()));
      }

      for (int i = 0; i < reads.size(); ++i) {
        final LoadOp<?> loadOp = loadOps.get(i);
        if (null == reads.get(i)) {
          throw new NotFoundException(String.format("Unable to find requested ref with ID: %s", loadOp.getId()));
        }

        final ValueType type = loadOp.getValueType();
        loadOp.loaded(type.addType(type.getSchema().itemToMap(VALUE_SERDE.deserialize(reads.get(i)), true)));
      }
    }
  }

  @Override
  public <V> boolean putIfAbsent(ValueType type, V value) {
    typeCheck(type, value);
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    typeCheck(type, value);

    // TODO: Handle ConditionExpressions.
    if (conditionUnAliased.isPresent()) {
      throw new UnsupportedOperationException("ConditionExpressions are not supported with RocksDB yet.");
    }

    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(type);
    final HasId valueAsHasId = (HasId)value;

    try {
      rocksDB.put(columnFamilyHandle, valueAsHasId.getId().toBytes(), VALUE_SERDE.serialize(type, value));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    final ListMultimap<ColumnFamilyHandle, SaveOp<?>> mm = Multimaps.index(ops, l -> getColumnFamilyHandle(l.getType()));

    try {
      final WriteBatch batch = new WriteBatch();
      for (Map.Entry<ColumnFamilyHandle, SaveOp<?>> entry : mm.entries()) {
        final SaveOp<?> op = entry.getValue();
        batch.put(entry.getKey(), op.getValue().getId().toBytes(), VALUE_SERDE.serialize(op.getType(), op.getValue()));
      }
      rocksDB.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <V> V loadSingle(ValueType valueType, Id id) throws NotFoundException {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(valueType);
    try {
      final byte[] buffer = rocksDB.get(columnFamilyHandle, id.toBytes());
      if (null == buffer) {
        throw new NotFoundException("Unable to load item with ID: " + id);
      }
      return VALUE_SERDE.deserialize(buffer);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<InternalRef> getRefs() {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(ValueType.REF);

    // TODO: Do we need to lock the database at all? Yes, use of Iterator must be synchronized.
    // Note: this is being refactored to getValues().
    final Iterable<InternalRef> iterable = () -> new AbstractIterator<InternalRef>() {
      private final RocksIterator itr = rocksDB.newIterator(columnFamilyHandle);
      private boolean isFirst = true;

      @Override
      protected InternalRef computeNext() {
        if (isFirst) {
          itr.seekToFirst();
          isFirst = false;
        } else {
          itr.next();
        }

        if (itr.isValid()) {
          return VALUE_SERDE.deserialize(itr.value());
        }

        itr.close();
        return endOfData();
      }
    };

    return StreamSupport.stream(iterable.spliterator(), false);
  }

  /**
   * Delete all the data in all column families, used for testing only.
   */
  @VisibleForTesting
  void deleteAllData() {
    // RocksDB doesn't expose a way to get the min/max key for a column family, so just use the min/max possible.
    byte[] minId = new byte[20];
    byte[] maxId = new byte[20];
    Arrays.fill(minId, (byte)0);
    Arrays.fill(maxId, (byte)255);

    for (ColumnFamilyHandle handle : valueTypeToColumnFamily.values()) {
      try {
        rocksDB.deleteRange(handle, minId, maxId);
        // Since RocksDB#deleteRange() is exclusive of the max key, delete it to ensure the column family is empty.
        rocksDB.delete(maxId);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Check if a given value is represented by the given type.
   * @param type the type of the value.
   * @param value the value to check.
   * @param <V> the type of the value.
   */
  static <V> void typeCheck(ValueType type, V value) {
    Preconditions.checkArgument(type.getObjectClass().isAssignableFrom(value.getClass()),
        "ValueType %s doesn't extend expected type %s.", value.getClass().getName(), type.getObjectClass().getName());
  }

  @VisibleForTesting
  ColumnFamilyHandle getColumnFamilyHandle(ValueType valueType) {
    final ColumnFamilyHandle columnFamilyHandle = valueTypeToColumnFamily.get(valueType);
    if (null == columnFamilyHandle) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return columnFamilyHandle;
  }

  private List<ColumnFamilyDescriptor> getColumnFamilies(String dbPath) {
    List<byte[]> columnFamilies = null;
    try (final Options options = new Options().setCreateIfMissing(true)) {
      columnFamilies = RocksDB.listColumnFamilies(options, dbPath);

      if (!columnFamilies.isEmpty() && !COLUMN_FAMILIES.equals(columnFamilies)) {
        throw new RuntimeException(String.format("Unexpected format for Nessie database at '%s'.", dbPath));
      }
    } catch (RocksDBException e) {
      LOGGER.warn("Error listing column families for Nessie database, using defaults.", e);
    }

    if (columnFamilies == null || columnFamilies.isEmpty()) {
      columnFamilies = COLUMN_FAMILIES;
    }

    return columnFamilies.stream()
      .map(c -> new ColumnFamilyDescriptor(c, new ColumnFamilyOptions().optimizeUniversalStyleCompaction()))
      .collect(Collectors.toList());
  }

  private String verifyPath() {
    final File dbDirectory = new File(this.dbDirectory);
    if (dbDirectory.exists()) {
      if (!dbDirectory.isDirectory()) {
        throw new RuntimeException(
          String.format("Invalid path '%s' for Nessie database, not a directory.", dbDirectory.getAbsolutePath()));
      }
    } else if (!dbDirectory.mkdirs()) {
      throw new RuntimeException(
          String.format("Failed to create directory '%s' for Nessie database.", dbDirectory.getAbsolutePath()));
    }

    return dbDirectory.getAbsolutePath();
  }
}
