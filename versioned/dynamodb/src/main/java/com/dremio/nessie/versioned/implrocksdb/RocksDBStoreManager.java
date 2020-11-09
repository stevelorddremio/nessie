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
package com.dremio.nessie.versioned.implrocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RocksDBStoreManager implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStoreManager.class);

  public static final byte[] REF_COLUMN_FAMILY = "ref".getBytes();
  public static final byte[] L1_COLUMN_FAMILY = "l1".getBytes();
  public static final byte[] L2_COLUMN_FAMILY = "l2".getBytes();
  public static final byte[] L3_COLUMN_FAMILY = "l3".getBytes();
  public static final byte[] VALUE_COLUMN_FAMILY = "value".getBytes();
  public static final byte[] KEY_FRAGMENT_COLUMN_FAMILY = "keyfragment".getBytes();
  public static final byte[] COMMIT_METADATA_AUTHOR_COLUMN_FAMILY = "commitmetadataauthor".getBytes();
  public static final byte[] COMMIT_METADATA_MESSAGE_COLUMN_FAMILY = "commitmetadatamessage".getBytes();
  private static ArrayList<byte[]> COLUMN_FAMILIES = new ArrayList<byte[]>() {{
      add(RocksDB.DEFAULT_COLUMN_FAMILY);
      add(REF_COLUMN_FAMILY);
      add(L1_COLUMN_FAMILY);
      add(L2_COLUMN_FAMILY);
      add(L3_COLUMN_FAMILY);
      add(VALUE_COLUMN_FAMILY);
      add(KEY_FRAGMENT_COLUMN_FAMILY);
      add(COMMIT_METADATA_AUTHOR_COLUMN_FAMILY);
      add(COMMIT_METADATA_MESSAGE_COLUMN_FAMILY);
    }};
  private static final long ROCKSDB_OPEN_SLEEP_MILLIS = 100L;
  private final String baseDirectory;

  private RocksDB db;
  private ColumnFamilyHandle defaultHandle;
  // the Options class contains a set of configurable DB options
  // that determines the behaviour of the database.
  // private Options options;
  private boolean noDBOpenRetry;
  // on #start all the existing tables are loaded, and if new stores are requested, #newStore is used
  private final ConcurrentMap<Integer, String> handleIdToNameMap = Maps.newConcurrentMap();
  private final LoadingCache<String, RocksDBStore> maps = CacheBuilder.newBuilder()
      .removalListener((RemovalListener<String, RocksDBStore>) notification -> {
        try {
          notification.getValue().close();
        } catch (Exception ex) {
          //TODO add exception
          //closeException.addException(ex);
        }
      }).build(new CacheLoader<String, RocksDBStore>() {
        @Override
        public RocksDBStore load(String name) throws RocksDBException {
          return newStore(name);
        }
      });

  public RocksDBStoreManager(String baseDirectory, boolean noDBOpenRetry) {
    this.baseDirectory = baseDirectory;
    this.noDBOpenRetry = noDBOpenRetry;
  }

  private RocksDBStore newStore(String name) throws RocksDBException {
    final ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name.getBytes(UTF_8));
    ColumnFamilyHandle handle = db.createColumnFamily(columnFamilyDescriptor);
    handleIdToNameMap.put(handle.getID(), name);

    return newRocksDBStore(name, columnFamilyDescriptor, handle);
  }

  private RocksDBStore newRocksDBStore(String name, ColumnFamilyDescriptor columnFamilyDescriptor,
                                       ColumnFamilyHandle handle) {
    /*
    final RocksMetaManager rocksManager;
    if (BLOB_WHITELIST.contains(name)) {
      rocksManager = new RocksMetaManager(baseDirectory, name, FILTER_SIZE_IN_BYTES);
    } else {
      rocksManager = new RocksMetaManager(baseDirectory, name, Long.MAX_VALUE);
    }
    */
    return new RocksDBStore(name, columnFamilyDescriptor, handle, db);
  }

  /**
   * This method opens the RocksDB database. It creates a cache of the tables(Column Families) created.
   */
  @Override
  public void start() {
    //TODO need to make path configurable.
    List<byte[]> families = null;

    //TODO implement valiation of path existing

    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      // get a list of existing families.
      families = new ArrayList<>(RocksDB.listColumnFamilies(options, baseDirectory));
    } catch (RocksDBException e) {
      LOGGER.error("Error extracting RocksDB options", e);
    }

    // if empty, add the default family
    if (families != null && families.isEmpty()) {
      families.addAll(COLUMN_FAMILIES);
    }
    final Function<byte[], ColumnFamilyDescriptor> func = ColumnFamilyDescriptor::new;

    List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
    try (final DBOptions dboptions = new DBOptions()) {
      dboptions.setCreateIfMissing(true);

      // TODO what options need to be set?

      // registerMetrics(dboptions);
      try {
        db = openDB(dboptions, baseDirectory, new ArrayList<>(Lists.transform(families, func)), familyHandles);

      } catch (RocksDBException e) {
        LOGGER.error("Error opening RocksDB backing store", e);
      }
    }
    // populate the local cache with the existing tables.
    for (int i = 0; i < families.size(); i++) {
      byte[] family = families.get(i);
      if (Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)) {
        defaultHandle = familyHandles.get(i);
      } else {
        String name = new String(family, UTF_8);
        final ColumnFamilyHandle handle = familyHandles.get(i);
        handleIdToNameMap.put(handle.getID(), name);
        RocksDBStore store = newRocksDBStore(name, new ColumnFamilyDescriptor(family), handle);
        maps.put(name, store);
      }
    }
  }

  /**
   * Opens or creates a Rocks DB.
   *
   * @param dboptions     the options with which to open the database
   * @param path          where the database is located
   * @param columnNames   an array of columns in the database
   * @param familyHandles the handles of the column families.
   * @return the instance of RocksDB created or opened
   * @throws RocksDBException Exception with RocksDB
   */
  public RocksDB openDB(final DBOptions dboptions, final String path, final List<ColumnFamilyDescriptor> columnNames,
                        List<ColumnFamilyHandle> familyHandles) throws RocksDBException {
    boolean printLockMessage = true;

    while (true) {
      try {
        return RocksDB.open(dboptions, path, columnNames, familyHandles);
      } catch (RocksDBException e) {
        if (e.getStatus().getCode() != Status.Code.IOError || !e.getStatus().getState().contains("While lock")) {
          throw e;
        }

        // Don't retry if request came from a CLI command (noDBOpenRetry = true)
        if (noDBOpenRetry) {
          LOGGER.error("Lock file to RocksDB is currently held by another process.  Stop other process before retrying.");
          System.out.println("Lock file to RocksDB is currently held by another process.  Stop other process before retrying.");
          throw e;
        }

        if (printLockMessage) {
          LOGGER.info("Lock file to RocksDB is currently held by another process. Will wait until lock is freed.");
          System.out.println("Lock file to RocksDB is currently held by another process. Will wait until lock is freed.");
          printLockMessage = false;
        }
      }

      // Add some wait until the next attempt
      try {
        TimeUnit.MILLISECONDS.sleep(ROCKSDB_OPEN_SLEEP_MILLIS);
      } catch (InterruptedException e) {
        throw new RocksDBException(new Status(Status.Code.TryAgain, Status.SubCode.None, "While open db"));
      }
    }
  }

  @Override
  public void close() {
    maps.invalidateAll();
  }

  @Override
  public void load(LoadStep loadstep) throws ReferenceNotFoundException {
  }

  @Override
  public <V> boolean putIfAbsent(ValueType type, V value) {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    return true;
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V loadSingle(ValueType valueType, Id id) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws ReferenceNotFoundException {
    try {
      int i = 0;
    } catch (Exception ex) {
      throw new ReferenceNotFoundException("Unable to find value.", ex);
    }
    return Optional.empty();
  }

  @Override
  public Stream<InternalRef> getRefs() {
    return null;
  }
}
