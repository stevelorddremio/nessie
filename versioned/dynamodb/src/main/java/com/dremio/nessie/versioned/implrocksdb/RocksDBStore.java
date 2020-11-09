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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
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

public class RocksDBStore implements Store {

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

  @Override
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
