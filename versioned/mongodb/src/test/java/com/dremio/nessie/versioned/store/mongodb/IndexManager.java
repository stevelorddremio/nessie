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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * A singleton class that establishes sharding and indexes on a set of collections.
 */
public class IndexManager {
  private static final Logger logger = Logger.getLogger(IndexManager.class.getName());

  // Singleton instance of this class.
  private static IndexManager instance = new IndexManager();

  /**
   * Private constructor.
   */
  private IndexManager() {
  }

  /**
   * Provides a singleton instance of this class.
   * @return the class instance.
   */
  public static IndexManager getInstance() {
    return instance;
  }

  /**
   * Establishes sharding and indexes on a set of collections.
   * @param adminDB the admin database required to invoke sharding.
   * @param db the database containing collections to be sharded.
   * @param mongoCollections the collections to be sharded.
   */
  public void createIndexOnCollection(MongoDatabase adminDB, MongoDatabase db, Set<MongoCollection> mongoCollections) {

    // enable sharding on the database
    adminDB.runCommand(new org.bson.Document("enableSharding", db.getName()));

    List<String> keys = new ArrayList<>();
    keys.add("_id.id");
    Index index = new Index(keys, false, true);
    List<Index> indexList = new ArrayList<>();
    indexList.add(index);
    final CollectionSharding metadata = new CollectionSharding(index, indexList);
    mongoCollections.forEach(collection -> {
      createIndexOnCollection(adminDB, collection, metadata);
    });
  }

  private void createIndexOnCollection(MongoDatabase adminDB,
                                       MongoCollection mongoCollection, CollectionSharding indexInfo) {
    final Index shardKey = indexInfo.getShardKey();

    // create shard key
    if (shardKey != null) {
      final List<Bson> fieldKeys = new ArrayList<>();

      shardKey.getKeys().forEach(entry -> {
        fieldKeys.add(Indexes.ascending(entry));
      });

      final Bson combinedKey = Indexes.compoundIndex(fieldKeys);
      final org.bson.Document d = new org.bson.Document("shardCollection", mongoCollection.getNamespace().getFullName());
      d.append("key", combinedKey);

      Publisher<Document> result = adminDB.runCommand(d);
      logger.info(String.format("Created ShardKey: %s on collection: %s",
          result.toString(), mongoCollection.getNamespace().getFullName()));
    }

    //create rest of the index
    final List<Index> indexes = indexInfo.getCompoundIndexInfo();
    for (Index index : indexes) {

      final List<Bson> fieldKeys = new ArrayList<>();
      index.getKeys().forEach((entry) -> {
        fieldKeys.add(index.isAscending() ? Indexes.ascending(entry) : Indexes.descending(entry));
      });

      final Bson combinedKey = Indexes.compoundIndex(fieldKeys);
      final Publisher<String> iName = mongoCollection.createIndex(combinedKey, new IndexOptions()
              .unique(index.isUnique())
              .background(true));

      logger.info(String.format("Created Index: %s on collection: %s isUnique: %s", iName, mongoCollection.getNamespace(),
          index.isUnique()));
    } //for
  }
}
