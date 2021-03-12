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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L1;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L1} providing
 * SerDe and Condition evaluation.
 *
 * <p>Conceptually, this is matching the following JSON structure:</p>
 * <pre>{
 *   "id": &lt;ByteString&gt;,         // ID
 *   "dt": &lt;int64&gt;,              // DATETIME
 *   "metadataId": &lt;ByteString&gt;, // COMMIT_METADATA
 *   "ancestors": [                    // ANCESTORS
 *     &lt;ByteString&gt;
 *   ],
 *   "tree": [                         // TREE
 *     &lt;ByteString&gt;
 *   ],
 *   "mutations": [                    // KEY_MUTATIONS
 *     {
 *       0: [                          // Addition (any number of these)
 *         &lt;String&gt;
 *       ],
 *     },
 *     {
 *       1: [                          // Removal (any number of these)
 *         &lt;String&gt;
 *       ]
 *     }
 *   ],
 *   "origin": &lt;ByteString&gt;,     // CHECKPOINT_ID
 *   "dist": &lt;int32&gt;,            // DISTANCE_FROM_CHECKPOINT
 *   "fragments": [                    // COMPLETE_KEY_LIST
 *     &lt;ByteString&gt;
 *   ]
 * }</pre>
 *
 * <p>NOTE: Will either contain both origin and dist, or just fragments. Cannot contain all three
 * fields.
 */
class RocksL1 extends RocksBaseValue<L1> implements L1 {

  static final int SIZE = 43;
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "parents";
  static final String TREE = "tree";
  static final String KEY_MUTATIONS = "mutations";
  static final String KEY_MUTATIONS_KEY = "key";
  static final String COMPLETE_KEY_LIST = "fragments";
  static final String CHECKPOINT_ID = "origin";
  static final String DISTANCE_FROM_CHECKPOINT = "dist";
  static final PathPattern ANCESTORS_EXACT = PathPattern.exact(ANCESTORS);
  static final PathPattern ANCESTORS_INDEX_EXACT = PathPattern.exact(ANCESTORS).anyPosition();
  static final PathPattern TREE_EXACT = PathPattern.exact(TREE);
  static final PathPattern TREE_INDEX_EXACT = PathPattern.exact(TREE).anyPosition();
  static final PathPattern KEY_MUTATIONS_EXACT = PathPattern.exact(KEY_MUTATIONS);
  static final PathPattern KEY_MUTATIONS_INDEX_EXACT = PathPattern.exact(KEY_MUTATIONS).anyPosition();
  static final PathPattern KEY_MUTATIONS_INDEX_PREFIX = PathPattern.prefix(KEY_MUTATIONS).anyPosition();
  static final PathPattern KEY_MUTATIONS_KEY_FULL_EXACT = PathPattern.exact(KEY_MUTATIONS).anyPosition().nameEquals(KEY_MUTATIONS_KEY);
  static final PathPattern KEY_MUTATIONS_KEY_INDEX_FULL_EXACT = PathPattern.exact(KEY_MUTATIONS).anyPosition()
      .nameEquals(KEY_MUTATIONS_KEY).anyPosition();
  static final PathPattern KEY_MUTATIONS_KEY_EXACT = PathPattern.exact(KEY_MUTATIONS_KEY);
  static final PathPattern KEY_MUTATIONS_KEY_INDEX_EXACT = PathPattern.exact(KEY_MUTATIONS_KEY).anyPosition();
  static final PathPattern COMPLETE_KEY_LIST_EXACT = PathPattern.exact(COMPLETE_KEY_LIST);
  static final PathPattern COMPLETE_KEY_LIST_INDEX_EXACT = PathPattern.exact(COMPLETE_KEY_LIST).anyPosition();
  static final PathPattern CHECKPOINT_ID_EXACT = PathPattern.exact(CHECKPOINT_ID);
  static final PathPattern DISTANCE_FROM_CHECKPOINT_EXACT = PathPattern.exact(DISTANCE_FROM_CHECKPOINT);
  static final PathPattern COMMIT_METADATA_EXACT = PathPattern.exact(COMMIT_METADATA);

  private final ValueProtos.L1.Builder l1Builder = ValueProtos.L1.newBuilder();

  RocksL1() {
    super();
  }

  @Override
  public L1 commitMetadataId(Id id) {
    l1Builder.setMetadataId(id.getValue());
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    l1Builder.clearAncestors();
    ids.forEach(id -> l1Builder.addAncestors(id.getValue()));
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    l1Builder.clearTree();
    ids.forEach(id -> l1Builder.addTree(id.getValue()));
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    l1Builder.clearKeyMutations();

    keyMutations.forEach(km -> {
      final ValueProtos.KeyMutation.Builder keyMutationBuilder = ValueProtos.KeyMutation.newBuilder();
      keyMutationBuilder.setTypeValue(km.getType().ordinal());
      keyMutationBuilder.setKey(ValueProtos.Key.newBuilder().addAllElements(km.getKey().getElements()));

      l1Builder.addKeyMutations(keyMutationBuilder);
    });

    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    l1Builder.setIncrementalList(
        ValueProtos.IncrementalList
            .newBuilder()
            .setCheckpointId(checkpointId.getValue())
            .setDistanceFromCheckpointId(distanceFromCheckpoint)
            .build()
    );
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    final ValueProtos.CompleteList.Builder completeListBuilder = ValueProtos.CompleteList.newBuilder();
    fragmentIds.forEach(id -> completeListBuilder.addFragmentIds(id.getValue()));
    l1Builder.setCompleteList(completeListBuilder);
    return this;
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final ExpressionPath.NameSegment nameSegment = function.getRootPathAsNameSegment();
    final String segment = nameSegment.getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case COMMIT_METADATA:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !Id.of(l1Builder.getMetadataId()).toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case ANCESTORS:
        evaluate(function, l1Builder.getAncestorsList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case TREE:
        evaluate(function, l1Builder.getTreeList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case KEY_MUTATIONS:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
      case CHECKPOINT_ID:
        if (!Id.of(l1Builder.getIncrementalList().getCheckpointId()).toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case DISTANCE_FROM_CHECKPOINT:
        if (!Entity.ofNumber(l1Builder.getIncrementalList().getDistanceFromCheckpointId()).equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case COMPLETE_KEY_LIST:
        evaluate(function, l1Builder.getCompleteList().getFragmentIdsList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  protected void remove(ExpressionPath path) {
    // parents[*]
    if (path.accept(ANCESTORS_INDEX_EXACT)) {
      List<ByteString> updatedAncestors = new ArrayList<>(l1Builder.getAncestorsList());
      updatedAncestors.remove(getPathSegmentAsPosition(path, 1));
      l1Builder.clearAncestors().addAllAncestors(updatedAncestors);
    // tree[*]
    } else if (path.accept(TREE_INDEX_EXACT)) {
      List<ByteString> updatedChildren = new ArrayList<>(l1Builder.getTreeList());
      updatedChildren.remove(getPathSegmentAsPosition(path, 1));
      l1Builder.clearTree().addAllTree(updatedChildren);
    // mutations[*]
    } else if (path.accept(KEY_MUTATIONS_INDEX_EXACT)) {
      l1Builder.removeKeyMutations(getPathSegmentAsPosition(path, 1));
    // mutations[*]/key[*]
    } else if (path.accept(KEY_MUTATIONS_KEY_INDEX_FULL_EXACT)) {
      final int keyMutationsIndex = getPathSegmentAsPosition(path, 1);
      final int keyIndex = getPathSegmentAsPosition(path, 3);

      final List<String> updatedKeysList = new ArrayList<>(l1Builder.getKeyMutations(keyMutationsIndex).getKey().getElementsList());
      updatedKeysList.remove(keyIndex);
      l1Builder.setKeyMutations(keyMutationsIndex, ValueProtos.KeyMutation
          .newBuilder(l1Builder.getKeyMutations(keyMutationsIndex))
          .setKey(ValueProtos.Key.newBuilder().addAllElements(updatedKeysList))
      );
    // fragments[*]
    } else if (path.accept(COMPLETE_KEY_LIST_INDEX_EXACT)) {
      List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
      updatedKeyList.remove(getPathSegmentAsPosition(path, 1));
      l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for remove in L1", path.asString()));
    }
  }

  @Override
  protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
    if (path.accept(ANCESTORS_EXACT)) {
      valuesToAdd.forEach(e -> l1Builder.addAncestors(e.getBinary()));
    } else if (path.accept(TREE_EXACT)) {
      valuesToAdd.forEach(e -> l1Builder.addTree(e.getBinary()));
    } else if (path.accept(KEY_MUTATIONS_EXACT)) {
      valuesToAdd.forEach(e -> l1Builder.addTree(e.getBinary()));
    } else if (path.accept(KEY_MUTATIONS_KEY_FULL_EXACT)) {
      final int i = getPathSegmentAsPosition(path, 1);

      final ValueProtos.Key.Builder keyBuilder = ValueProtos.Key.newBuilder(l1Builder.getKeyMutations(i).getKey());
      valuesToAdd.forEach(e -> keyBuilder.addElements(e.getString()));
      l1Builder.setKeyMutations(i, ValueProtos.KeyMutation
          .newBuilder(l1Builder.getKeyMutations(i))
          .setKey(keyBuilder));
    } else if (path.accept(COMPLETE_KEY_LIST_EXACT)) {
      List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
      updatedKeyList.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));

      final ValueProtos.CompleteList.Builder completeListBuilder = ValueProtos.CompleteList.newBuilder();
      updatedKeyList.forEach(completeListBuilder::addFragmentIds);
      l1Builder.setCompleteList(completeListBuilder);
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for append in L1", path.asString()));
    }
  }

  @Override
  protected void set(ExpressionPath path, Entity newValue) {
    if (path.accept(ANCESTORS_EXACT)) {
      l1Builder.clearAncestors();
      newValue.getList().forEach(e -> l1Builder.addAncestors(e.getBinary()));
    } else if (path.accept(ANCESTORS_INDEX_EXACT)) {
      l1Builder.setAncestors(getPathSegmentAsPosition(path, 1), newValue.getBinary());
    } else if (path.accept(TREE_EXACT)) {
      l1Builder.clearTree();
      newValue.getList().forEach(e -> l1Builder.addTree(e.getBinary()));
    } else if (path.accept(TREE_INDEX_EXACT)) {
      l1Builder.setTree(getPathSegmentAsPosition(path, 1), newValue.getBinary());
    } else if (path.accept(KEY_MUTATIONS_EXACT)) {
      l1Builder.clearKeyMutations();
      newValue.getList().forEach(e -> l1Builder.addKeyMutations(EntityConverter.entityToKeyMutation(e)));
    } else if (path.accept(KEY_MUTATIONS_INDEX_EXACT)) {
      final int i = getPathSegmentAsPosition(path, 1);
      l1Builder.setKeyMutations(i, EntityConverter.entityToKeyMutation(newValue));
    } else if (path.accept(KEY_MUTATIONS_INDEX_PREFIX)) {
      setsKeyMutations(getPathSegmentAsPosition(path, 1), KEY_MUTATIONS_INDEX_PREFIX.removePrefix(path), newValue);
    } else if (path.accept(COMPLETE_KEY_LIST_EXACT)) {
      final ValueProtos.CompleteList.Builder completeListBuilder = ValueProtos.CompleteList.newBuilder();
      newValue.getList().forEach(e -> completeListBuilder.addFragmentIds(e.getBinary()));
      l1Builder.setCompleteList(completeListBuilder);
    } else if (path.accept(COMPLETE_KEY_LIST_INDEX_EXACT)) {
      final int i = getPathSegmentAsPosition(path, 1);
      l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder(l1Builder.getCompleteList()).setFragmentIds(i, newValue.getBinary()));
    } else if (path.accept(CHECKPOINT_ID_EXACT)) {
      l1Builder.setIncrementalList(ValueProtos.IncrementalList
          .newBuilder(l1Builder.getIncrementalList())
          .setCheckpointId(newValue.getBinary())
      );
    } else if (path.accept(DISTANCE_FROM_CHECKPOINT_EXACT)) {
      l1Builder.setIncrementalList(ValueProtos.IncrementalList
          .newBuilder(l1Builder.getIncrementalList())
          .setDistanceFromCheckpointId((int)newValue.getNumber())
      );
    } else if (path.accept(COMMIT_METADATA_EXACT)) {
      commitMetadataId(Id.of(newValue.getBinary()));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for set equals in L1", path.asString()));
    }
  }

  void setsKeyMutations(int keyMutationIndex, ExpressionPath path, Entity newValue) {
    if (path.accept(KEY_MUTATIONS_KEY_EXACT)) {
      l1Builder.setKeyMutations(keyMutationIndex, ValueProtos.KeyMutation
          .newBuilder(l1Builder.getKeyMutations(keyMutationIndex))
          .setKey(EntityConverter.entityToKey(newValue)));
    } else if (path.accept(KEY_MUTATIONS_KEY_INDEX_EXACT)) {
      final int keyIndex = getPathSegmentAsPosition(path, 1);

      l1Builder.setKeyMutations(keyMutationIndex, ValueProtos.KeyMutation
          .newBuilder(l1Builder.getKeyMutations(keyMutationIndex))
          .setKey(ValueProtos.Key
            .newBuilder(l1Builder.getKeyMutations(keyMutationIndex).getKey())
            .setElements(keyIndex, newValue.getString())));
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a valid path for set equals in L1.keyMutations[%d]", path.asString(), keyMutationIndex));
    }
  }

  @Override
  byte[] build() {
    checkPresent(l1Builder.getMetadataId(), COMMIT_METADATA);
    checkPresent(l1Builder.getAncestorsList(), ANCESTORS);
    checkPresent(l1Builder.getTreeList(), TREE);
    checkPresent(l1Builder.getKeyMutationsList(), KEY_MUTATIONS);

    if (l1Builder.hasIncrementalList()) {
      checkPresent(l1Builder.getIncrementalList().getCheckpointId(), CHECKPOINT_ID);
    } else {
      checkPresent(l1Builder.getCompleteList().getFragmentIdsList(), COMPLETE_KEY_LIST);
    }

    return l1Builder.setBase(buildBase()).build().toByteArray();
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
        consumer.completeKeyList(l1.getCompleteList().getFragmentIdsList().stream().map(Id::of));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L1 value encountered when deserializing.", e);
    }
  }

  Id getMetadataId() {
    return Id.of(l1Builder.getMetadataId());
  }

  Id getCheckpointId() {
    return Id.of(l1Builder.getIncrementalList().getCheckpointId());
  }

  int getDistanceFromCheckpoint() {
    return l1Builder.getIncrementalList().getDistanceFromCheckpointId();
  }

  Stream<Id> getChildren() {
    return l1Builder.getTreeList().stream().map(Id::of);
  }

  Stream<Id> getAncestors() {
    return l1Builder.getAncestorsList().stream().map(Id::of);
  }

  Stream<Id> getCompleteKeyList() {
    return l1Builder.getCompleteList().getFragmentIdsList().stream().map(Id::of);
  }

  int getKeyMutationsCount() {
    return l1Builder.getKeyMutationsCount();
  }

  int getKeyMutationType(int index) {
    return l1Builder.getKeyMutations(index).getTypeValue();
  }

  List<String> getKeyMutationKeys(int index) {
    return l1Builder.getKeyMutations(index).getKey().getElementsList();
  }
}
