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

import java.util.concurrent.atomic.AtomicBoolean;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDBStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStore.class);

  private static final long ROCKSDB_OPEN_SLEEP_MILLIS = 100L;
  private ColumnFamilyHandle handle;

  private final ColumnFamilyDescriptor family;
  private final String name;


  public final RocksDB db;

  //Location of the database file
  private String path;
  // The closed state of the RocksRB instance.
  private final AtomicBoolean closed = new AtomicBoolean(true);

  static {
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();
  }

  /**
   * create a DynamoStore.
   */
  // public RocksDBStore(DynamoStoreConfig config) {
  //public RocksDBStore(Options options, String path) {
  public RocksDBStore(String name, ColumnFamilyDescriptor family, ColumnFamilyHandle handle, RocksDB db) {
    // this.options = options;
    this.family = family;
    this.name = name;
    this.db = db;
    this.handle = handle;
  }

  public void start() {
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try (FlushOptions options = new FlushOptions()) {
      options.setWaitForFlush(true);
      db.flush(options, handle);
    } catch (RocksDBException ex) {
      // deferred.addException(ex);
    }
    db.close();
  }
}
