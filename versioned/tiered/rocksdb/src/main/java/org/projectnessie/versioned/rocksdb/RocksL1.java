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
 * Conceptually, this is matching the following JSON structure:
 * {
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
 *       "MutationType": &lt;int&gt;,  // KEY_MUTATIONS_MUTATION_TYPE
 *       "key": [                      // KEY_MUTATIONS_KEY
 *         &lt;String&gt;
 *       ]
 *     }
 *   ],
 *   "origin": &lt;ByteString&gt;,     // CHECKPOINT_ID
 *   "dist": &lt;int32&gt;,            // DISTANCE_FROM_CHECKPOINT
 *   "fragments": [                    // COMPLETE_KEY_LIST
 *     &lt;ByteString&gt;
 *   ]
 * }
 *
 * NOTE: Will either contain origin and dist, or it will fragments. Cannot contain all three
 * fields.
 */
class RocksL1 extends RocksBaseValue<L1> implements L1 {

  static final int SIZE = 43;
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "parents";
  static final String TREE = "tree";
  static final String KEY_MUTATIONS = "mutations";
  static final String KEY_MUTATIONS_MUTATION_TYPE = "mutationType";
  static final String KEY_MUTATIONS_KEY = "key";
  static final String COMPLETE_KEY_LIST = "fragments";
  static final String CHECKPOINT_ID = "origin";
  static final String DISTANCE_FROM_CHECKPOINT = "dist";

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
    l1Builder
        .clearAncestors()
        .addAllAncestors(ids.map(Id::getValue).collect(Collectors.toList()));
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    l1Builder
        .clearTree()
        .addAllTree(ids.map(Id::getValue).collect(Collectors.toList()));
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    l1Builder
        .clearKeyMutations()
        .addAllKeyMutations(
            keyMutations.map(km ->
                ValueProtos.KeyMutation
                    .newBuilder()
                    .setTypeValue(km.getType().ordinal())
                    .setKey(
                        ValueProtos.Key.newBuilder().addAllElements(km.getKey().getElements()).build()
                    )
                    .build())
            .collect(Collectors.toList())
        );
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
    l1Builder.setCompleteList(
        ValueProtos.CompleteList
          .newBuilder()
          .addAllFragmentIds(fragmentIds.map(Id::getValue).collect(Collectors.toList()))
          .build()
    );
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
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    switch (fieldName) {
      case ANCESTORS:
        List<ByteString> updatedAncestors = new ArrayList<>(l1Builder.getAncestorsList());
        updatedAncestors.remove(getPosition(path));
        l1Builder.clearAncestors().addAllAncestors(updatedAncestors);
        break;
      case TREE:
        List<ByteString> updatedChildren = new ArrayList<>(l1Builder.getTreeList());
        updatedChildren.remove(getPosition(path));
        l1Builder.clearTree().addAllTree(updatedChildren);
        break;
      case KEY_MUTATIONS:
        removesKeyMutations(path);
        break;
      case COMPLETE_KEY_LIST:
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.remove(getPosition(path));
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  private void removesKeyMutations(ExpressionPath.PathSegment path) {
    if (path.isPosition() && !path.getChild().isPresent()) {
      l1Builder.removeKeyMutations(getPosition(path));
    } else if (path.isPosition()) {
      if (!path.getChild().get().isName() || !KEY_MUTATIONS_KEY.equals(path.getChild().get().asName().getName())) {
        throw new UnsupportedOperationException("Invalid path for remove");
      }

      final ExpressionPath.PathSegment keyPath = path.getChild().get();
      if (!keyPath.getChild().isPresent() || !keyPath.getChild().get().isPosition()) {
        throw new UnsupportedOperationException("Invalid path for remove");
      }

      final List<String> updatedKeysList = new ArrayList<>(l1Builder.getKeyMutations(getPosition(path)).getKey().getElementsList());
      updatedKeysList.remove(getPosition(keyPath.getChild().get()));
      l1Builder.setKeyMutations(getPosition(path), ValueProtos.KeyMutation
          .newBuilder(l1Builder.getKeyMutations(getPosition(path)))
          .setKey(ValueProtos.Key.newBuilder().addAllElements(updatedKeysList))
      );
    } else {
      throw new UnsupportedOperationException(String.format("Remove not supported for %s", KEY_MUTATIONS));
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    switch (fieldName) {
      case ANCESTORS:
      case TREE:
      case COMPLETE_KEY_LIST:
        return childPath == null || childPath.isPosition();
      case KEY_MUTATIONS:
        if (childPath == null || childPath.isPosition()) {
          return true;
        } else if (KEY_MUTATIONS_KEY.equals(childPath.asName().getName())) {
          return !childPath.getChild().isPresent() || childPath.getChild().get().isPosition();
        }
        return false;
      default:
        return false;
    }
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    switch (fieldName) {
      case ANCESTORS:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for append");
        }
        l1Builder.addAllAncestors(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case TREE:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for append");
        }
        l1Builder.addAllTree(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case KEY_MUTATIONS:
        if (!childPath.isPosition() || !childPath.getChild().isPresent() || !childPath.getChild().get().isName()
            || !KEY_MUTATIONS_KEY.equals(childPath.getChild().get().asName().getName())) {
          throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
        }

        int keyMutationPosition = childPath.asPosition().getPosition();
        List<String> keyElements = new ArrayList<>(l1Builder.getKeyMutations(keyMutationPosition).getKey().getElementsList());
        keyElements.addAll(valuesToAdd.stream().map(Entity::getString).collect(Collectors.toList()));

        l1Builder.setKeyMutations(keyMutationPosition, ValueProtos.KeyMutation
            .newBuilder(l1Builder.getKeyMutations(keyMutationPosition))
            .setKey(ValueProtos.Key.newBuilder().addAllElements(keyElements))
        );
        break;
      case COMPLETE_KEY_LIST:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for append");
        }
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    switch (fieldName) {
      case ANCESTORS:
        if (childPath != null) {
          l1Builder.setAncestors(getPosition(childPath), newValue.getBinary());
        } else {
          l1Builder.clearAncestors().addAllAncestors(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()));
        }
        break;
      case TREE:
        if (childPath != null) {
          l1Builder.setTree(getPosition(childPath), newValue.getBinary());
        } else {
          l1Builder.clearTree().addAllTree(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()));
        }
        break;
      case KEY_MUTATIONS:
        setsKeyMutations(childPath, newValue);
        break;
      case COMPLETE_KEY_LIST:
        final List<ByteString> updatedKeyList;
        if (childPath != null) {
          updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
          updatedKeyList.set(getPosition(childPath), newValue.getBinary());
        } else {
          updatedKeyList = newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList());
        }
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      case CHECKPOINT_ID:
        l1Builder.setIncrementalList(ValueProtos.IncrementalList
            .newBuilder(l1Builder.getIncrementalList())
            .setCheckpointId(newValue.getBinary())
        );
        break;
      case DISTANCE_FROM_CHECKPOINT:
        l1Builder.setIncrementalList(ValueProtos.IncrementalList
            .newBuilder(l1Builder.getIncrementalList())
            .setDistanceFromCheckpointId((int)newValue.getNumber())
        );
        break;
      case COMMIT_METADATA:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        commitMetadataId(Id.of(newValue.getBinary()));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  private void setsKeyMutations(ExpressionPath.PathSegment childPath, Entity newValue) {
    if (childPath == null || !childPath.isPosition() || !childPath.getChild().isPresent() || !childPath.getChild().get().isName()) {
      throw new UnsupportedOperationException(String.format("Update not supported for %s", KEY_MUTATIONS));
    }

    int keyMutationPosition = childPath.asPosition().getPosition();

    switch (childPath.getChild().get().asName().getName()) {
      case KEY_MUTATIONS_MUTATION_TYPE:
        l1Builder.setKeyMutations(keyMutationPosition, ValueProtos.KeyMutation
            .newBuilder(l1Builder.getKeyMutations(keyMutationPosition))
            .setTypeValue((int)newValue.getNumber())
        );
        break;
      case KEY_MUTATIONS_KEY:
        if (!childPath.getChild().get().getChild().isPresent()) {
          l1Builder.setKeyMutations(keyMutationPosition, ValueProtos.KeyMutation
              .newBuilder(l1Builder.getKeyMutations(keyMutationPosition))
              .setKey(ValueProtos.Key.newBuilder().addAllElements(
              newValue.getList().stream().map(Entity::getString).collect(Collectors.toList())
            ))
          );
        } else if (childPath.getChild().get().getChild().isPresent() && childPath.getChild().get().getChild().get().isPosition()) {
          List<String> keyElements = new ArrayList<>(l1Builder.getKeyMutations(keyMutationPosition).getKey().getElementsList());
          keyElements.set(childPath.getChild().get().getChild().get().asPosition().getPosition(), newValue.getString());

          l1Builder.setKeyMutations(keyMutationPosition, ValueProtos.KeyMutation
              .newBuilder(l1Builder.getKeyMutations(keyMutationPosition))
              .setKey(ValueProtos.Key.newBuilder().addAllElements(keyElements))
          );
        } else {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("\"%s\" does not contain field \"%s\"", KEY_MUTATIONS, childPath.getChild().get().asName().getName())
        );
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
