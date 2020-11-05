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
import java.util.stream.Stream;

import org.rocksdb.Options;
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
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;

public class RocksDBStore implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoStore.class);

  private RocksDB db;
  // the Options class contains a set of configurable DB options
  // that determines the behaviour of the database.
  private Options options;

  //Location of the database file
  private String path;

  // private final DynamoStoreConfig config;
  // private final RocksDB db;

  // private DynamoDbClient client;
  // private DynamoDbAsyncClient async;
  // private final ImmutableMap<ValueType, String> tableNames;

  /**
   * create a DynamoStore.
   */
  // public RocksDBStore(DynamoStoreConfig config) {
  public RocksDBStore(Options options, String path) {
    this.options = options;
    this.path = path;

    if (options == null) {
      options = new Options().setCreateIfMissing(true);
    }

    //  this.config = config;
    //    this.tableNames = ImmutableMap.<ValueType, String>builder()
    //      .put(ValueType.REF, config.getRefTableName())
    //      .put(ValueType.L1, config.getTreeTableName())
    //      .put(ValueType.L2, config.getTreeTableName())
    //      .put(ValueType.L3, config.getTreeTableName())
    //      .put(ValueType.VALUE, config.getValueTableName())
    //      .put(ValueType.KEY_FRAGMENT, config.getKeyListTableName())
    //      .put(ValueType.COMMIT_METADATA, config.getMetadataTableName())
    //      .build();

  }

  @Override
  public void start() {
    // a static method that loads the RocksDB C++ library.
    RocksDB.loadLibrary();

    // a factory method that returns a RocksDB instance
    // TODO retrieve path from property
    try {
      db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      // do some error handling
      LOGGER.error("RocksDBException.", e);
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
