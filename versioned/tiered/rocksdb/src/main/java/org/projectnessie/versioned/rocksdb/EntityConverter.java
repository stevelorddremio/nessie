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
import java.util.stream.Collectors;

import org.projectnessie.versioned.store.Entity;

public class EntityConverter {

  /**
   * This converts an Entity into Commit protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  public static ValueProtos.Commit entityToCommit(Entity entity) {
    if (!entity.getType().equals(Entity.EntityType.MAP)) {
      throw new UnsupportedOperationException();
    }
    ValueProtos.Commit.Builder builder = ValueProtos.Commit.newBuilder();

    for (Map.Entry<String, Entity> level0 : entity.getMap().entrySet()) {
      switch (level0.getKey()) {
        case RocksRef.COMMITS_ID:
          builder.setId(level0.getValue().getBinary());
          break;
        case RocksRef.COMMITS_PARENT:
          builder.setParent(level0.getValue().getBinary());
          break;
        case RocksRef.COMMITS_COMMIT:
          builder.setCommit(level0.getValue().getBinary());
          break;
        case RocksRef.COMMITS_DELTA:
          if (!level0.getValue().getType().equals(Entity.EntityType.LIST)) {
            throw new UnsupportedOperationException();
          }
          for (Entity level1 : level0.getValue().getList()) {
            builder.addDelta(entityToDelta(level1));
          }
          break;
        case RocksRef.COMMITS_KEY_LIST:
          if (!level0.getValue().getType().equals(Entity.EntityType.LIST)) {
            throw new UnsupportedOperationException();
          }
          for (Entity level1 : level0.getValue().getList()) {
            builder.addKeyMutation(entityToKeyMutation(level1));
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    return builder.build();
  }

  /**
   * This converts an Entity into Delta protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  public static ValueProtos.Delta entityToDelta(Entity entity) {
    if (!entity.getType().equals(Entity.EntityType.MAP)) {
      throw new UnsupportedOperationException();
    }

    ValueProtos.Delta.Builder builder = ValueProtos.Delta.newBuilder();

    for (Map.Entry<String, Entity> level0 : entity.getMap().entrySet()) {
      switch (level0.getKey()) {
        case RocksRef.COMMITS_POSITION:
          builder.setPosition((int) level0.getValue().getNumber());
          break;
        case RocksRef.COMMITS_OLD_ID:
          builder.setOldId(level0.getValue().getBinary());
          break;
        case RocksRef.COMMITS_NEW_ID:
          builder.setNewId(level0.getValue().getBinary());
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    return builder.build();
  }

  /**
   * This converts an Entity into KeyMutation protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  public static ValueProtos.KeyMutation entityToKeyMutation(Entity entity) {
    if (!entity.getType().equals(Entity.EntityType.MAP)) {
      throw new UnsupportedOperationException();
    }

    ValueProtos.KeyMutation.Builder builder = ValueProtos.KeyMutation.newBuilder();

    for (Map.Entry<String, Entity> level0 : entity.getMap().entrySet()) {
      switch (level0.getKey()) {
        case RocksRef.COMMITS_KEY_ADDITION:
          builder.setType(ValueProtos.KeyMutation.MutationType.ADDITION);
          builder.setKey(entityToKey(level0.getValue()));
          break;
        case RocksRef.COMMITS_KEY_REMOVAL:
          builder.setType(ValueProtos.KeyMutation.MutationType.REMOVAL);
          builder.setKey(entityToKey(level0.getValue()));
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    return builder.build();
  }

  /**
   * This converts an Entity into Key protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  public static ValueProtos.Key entityToKey(Entity entity) {
    if (!entity.getType().equals(Entity.EntityType.LIST)) {
      throw new UnsupportedOperationException();
    }

    return ValueProtos.Key.newBuilder().addAllElements(
      entity.getList().stream().map(Entity::getString).collect(Collectors.toList())).build();
  }

  /**
   * This converts an Entity into KeyDelta protobuf object.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  public static ValueProtos.KeyDelta entityToKeyDelta(Entity entity) {
    if (entity.getType() != Entity.EntityType.MAP) {
      throw new UnsupportedOperationException("Invalid value for keyDelta");
    }

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
