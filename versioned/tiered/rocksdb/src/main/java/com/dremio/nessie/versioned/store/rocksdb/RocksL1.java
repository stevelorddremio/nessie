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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

class RocksL1 extends RocksBaseValue<L1> implements L1, Evaluator {

  static final int SIZE = 43;
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "ancestors";
  static final String CHILDREN = "children";
  static final String KEY_LIST = "keylist";
  static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  static final String COMPLETE_KEY_LIST = "completeKeyList";
  static final String CHECKPOINT_ID = "checkpointId";
  static final String DISTANCE_FROM_CHECKPOINT = "distanceFromCheckpoint";

  private Id metadataId; // commitMetadataId
  private Stream<Id> parentList; // ancestors
  private Stream<Id> tree; // children

  private Stream<Key.Mutation> keyMutations; // keylist
  private Id checkpointId; // checkpointId
  private int distanceFromCheckpoint; // distanceFromCheckpoint
  private Stream<Id> fragmentIds; // completeKeyList

  RocksL1() {
    super();
  }

  @Override
  public L1 commitMetadataId(Id id) {
    this.metadataId = id;
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    this.parentList = ids;
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    this.tree = ids;
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    this.keyMutations = keyMutations;
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    this.checkpointId = checkpointId;
    this.distanceFromCheckpoint = distanceFromCheckpoint;
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    this.fragmentIds = fragmentIds;
    return this;
  }

  @Override
  public boolean evaluate(Condition condition) {
    boolean result = true;
    for (Function function: condition.functionList) {
      // Retrieve entity at function.path
      final List<String> path = Evaluator.splitPath(function.getPath());
      final String segment = path.get(0);
      if (segment.equals(ID)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (getId().toEntity().equals(function.getValue())));
      } else if (segment.equals(COMMIT_METADATA)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (metadataId.toEntity().equals(function.getValue())));
      } else if (segment.startsWith(ANCESTORS)) {
        result &= evaluateStream(function, parentList);
      } else if (segment.startsWith(CHILDREN)) {
        result &= evaluateStream(function, tree);
      } else if (segment.equals(KEY_LIST)) {
        return false;
      } else if (segment.equals(INCREMENTAL_KEY_LIST)) {
        if (path.size() == 2 && (function.getOperator().equals(Function.EQUALS))) {
          if (path.get(1).equals(CHECKPOINT_ID)) {
            result &= (checkpointId.toEntity().equals(function.getValue()));
          } else if (path.get(1).equals((DISTANCE_FROM_CHECKPOINT))) {
            result &= (Entity.ofNumber(distanceFromCheckpoint).equals(function.getValue()));
          } else {
            // Invalid Condition Function.
            return false;
          }
        } else {
          // Invalid Condition Function.
          return false;
        }
      } else if (segment.startsWith(COMPLETE_KEY_LIST)) {
        result &= evaluateStream(function, fragmentIds);
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }

  @Override
  byte[] build() {
    checkPresent(metadataId, COMMIT_METADATA);
    checkPresent(parentList, ANCESTORS);
    checkPresent(tree, CHILDREN);
    checkPresent(keyMutations, KEY_LIST);

    final ValueProtos.L1.Builder builder = ValueProtos.L1.newBuilder()
        .setBase(buildBase())
        .setMetadataId(metadataId.getValue())
        .addAllAncestors(buildIds(parentList))
        .addAllTree(buildIds(tree))
        .addAllKeyMutations(keyMutations.map(RocksBaseValue::buildKeyMutation).collect(Collectors.toList()));

    if (null == fragmentIds) {
      checkPresent(checkpointId, CHECKPOINT_ID);
      builder.setIncrementalList(ValueProtos.IncrementalList.newBuilder()
          .setCheckpointId(checkpointId.getValue())
          .setDistanceFromCheckpointId(distanceFromCheckpoint)
          .build());
    } else {
      checkPresent(fragmentIds, COMPLETE_KEY_LIST);
      builder.addAllFragmentIds(buildIds(fragmentIds));
    }

    return builder.build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L1 consumer) {
    try {
      final ValueProtos.L1 l1 = ValueProtos.L1.parseFrom(value);
      setBase(consumer, l1.getBase());
      consumer
          .commitMetadataId(Id.of(l1.getMetadataId()))
          .ancestors(l1.getAncestorsList().stream().map(Id::of))
          .children(l1.getTreeList().stream().map(Id::of))
          .keyMutations(l1.getKeyMutationsList().stream().map(RocksBaseValue::createKeyMutation));
      if (l1.hasIncrementalList()) {
        final ValueProtos.IncrementalList incList = l1.getIncrementalList();
        consumer.incrementalKeyList(Id.of(incList.getCheckpointId()), incList.getDistanceFromCheckpointId());
      } else {
        consumer.completeKeyList(l1.getFragmentIdsList().stream().map(Id::of));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L1 value encountered when deserializing.", e);
    }
  }
}