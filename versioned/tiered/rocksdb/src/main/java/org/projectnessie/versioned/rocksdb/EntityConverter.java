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

package org.projectnessie.versioned.rocksdb;

import java.util.Map;

import org.projectnessie.versioned.store.Entity;

import com.google.common.primitives.Ints;

/**
 * Static methods to use for converting Entity objects to Rocks protobuf objects.
 */
class EntityConverter {

  /**
   * This converts an Entity into Commit protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.Commit entityToCommit(Entity entity) {
    final ValueProtos.Commit.Builder builder = ValueProtos.Commit.newBuilder();

    for (Map.Entry<String, Entity> entry : entity.getMap().entrySet()) {
      switch (entry.getKey()) {
        case RocksRef.COMMITS_ID:
          builder.setId(entry.getValue().getBinary());
          break;
        case RocksRef.COMMITS_PARENT:
          builder.setParent(entry.getValue().getBinary());
          break;
        case RocksRef.COMMITS_COMMIT:
          builder.setCommit(entry.getValue().getBinary());
          break;
        case RocksRef.COMMITS_DELTA:
          entry.getValue().getList().forEach(e -> builder.addDelta(entityToDelta(e)));
          break;
        case RocksRef.COMMITS_KEY_LIST:
          entry.getValue().getList().forEach(e -> builder.addKeyMutation(entityToKeyMutation(e)));
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unknown field \"%s\" for keyDelta", entry.getKey()));
      }
    }
    return builder.build();
  }

  /**
   * This converts an Entity into Delta protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.Delta entityToDelta(Entity entity) {
    final ValueProtos.Delta.Builder builder = ValueProtos.Delta.newBuilder();

    for (Map.Entry<String, Entity> entry : entity.getMap().entrySet()) {
      switch (entry.getKey()) {
        case RocksRef.COMMITS_POSITION:
          builder.setPosition(Ints.saturatedCast(entry.getValue().getNumber()));
          break;
        case RocksRef.COMMITS_OLD_ID:
          builder.setOldId(entry.getValue().getBinary());
          break;
        case RocksRef.COMMITS_NEW_ID:
          builder.setNewId(entry.getValue().getBinary());
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unknown field \"%s\" for delta", entry.getKey()));
      }
    }
    return builder.build();
  }

  /**
   * This converts an Entity into KeyMutation protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.KeyMutation entityToKeyMutation(Entity entity) {
    final ValueProtos.KeyMutation.Builder builder = ValueProtos.KeyMutation.newBuilder();

    for (Map.Entry<String, Entity> entry : entity.getMap().entrySet()) {
      switch (entry.getKey()) {
        case RocksRef.COMMITS_KEY_ADDITION:
          builder.setType(ValueProtos.KeyMutation.MutationType.ADDITION);
          break;
        case RocksRef.COMMITS_KEY_REMOVAL:
          builder.setType(ValueProtos.KeyMutation.MutationType.REMOVAL);
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unknown field \"%s\" for keyMutation", entry.getKey()));
      }

      builder.setKey(entityToKey(entry.getValue()));
    }
    return builder.build();
  }

  /**
   * This converts an Entity into Key protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.Key entityToKey(Entity entity) {
    final ValueProtos.Key.Builder keyBuilder = ValueProtos.Key.newBuilder();
    entity.getList().forEach(e -> keyBuilder.addElements(e.getString()));

    return keyBuilder.build();
  }

  /**
   * This converts an Entity into KeyDelta protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.KeyDelta entityToKeyDelta(Entity entity) {
    final Map<String, Entity> entityMap = entity.getMap();
    final ValueProtos.KeyDelta.Builder keyDeltaBuilder = ValueProtos.KeyDelta.newBuilder();

    for (Map.Entry<String, Entity> entry : entityMap.entrySet()) {
      switch (entry.getKey()) {
        case RocksL3.TREE_ID:
          keyDeltaBuilder.setId(entry.getValue().getBinary());
          break;
        case RocksL3.TREE_KEY:
          keyDeltaBuilder.setKey(entityToKey(entry.getValue()));
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unknown field \"%s\" for keyDelta", entry.getKey()));
      }
    }

    return keyDeltaBuilder.build();
  }
}
