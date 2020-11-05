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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class RocksDBStore implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoStore.class);

  private static final long ROCKSDB_OPEN_SLEEP_MILLIS = 100L;

  private RocksDB db;
  // the Options class contains a set of configurable DB options
  // that determines the behaviour of the database.
  // private Options options;
  private boolean noDBOpenRetry;

  //Location of the database file
  private String path;

  static {
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();
  }

  /**
   * create a DynamoStore.
   */
  // public RocksDBStore(DynamoStoreConfig config) {
  public RocksDBStore(Options options, String path) {
    // this.options = options;
    this.path = path;
  }

  @Override
  public void start() {
    List<byte[]> families = null;
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      // get a list of existing families.
      families = new ArrayList<>(RocksDB.listColumnFamilies(options, path));
    } catch (RocksDBException e) {
      LOGGER.error("Error extracting RocksDB options", e);
    }

    // if empty, add the default family
    if (families != null && families.isEmpty()) {
      families.add(RocksDB.DEFAULT_COLUMN_FAMILY);
    }
    final Function<byte[], ColumnFamilyDescriptor> func = ColumnFamilyDescriptor::new;

    List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
    try (final DBOptions dboptions = new DBOptions()) {
      dboptions.setCreateIfMissing(true);

      // TODO what options need to be set?

      // registerMetrics(dboptions);
      try {
        db = openDB(dboptions, path, new ArrayList<>(Lists.transform(families, func)), familyHandles);

      } catch (RocksDBException e) {
        LOGGER.error("Error opening RocksDB backing store", e);
      }
    }
  }

  /**
   Opens or creates a Rocks DB.
   * @param dboptions the options with which to open the database
   * @param path where the database is located
   * @param columnNames an array of columns in the database
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
  //    client.close();
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
