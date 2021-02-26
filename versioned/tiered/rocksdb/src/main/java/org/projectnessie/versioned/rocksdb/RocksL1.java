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
import java.util.Objects;
import java.util.Optional;
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
 */
class RocksL1 extends RocksBaseValue<L1> implements L1 {

  static final int SIZE = 43;
  static final String COMMIT_METADATA = "metadataId";
  static final String ANCESTORS = "ancestors";
  static final String CHILDREN = "children";
  static final String KEY_LIST = "keylist";
  static final String INCREMENTAL_KEY_LIST = "incrementalKeyList";
  static final String COMPLETE_KEY_LIST = "completeKeyList";
  static final String CHECKPOINT_ID = "checkpointId";
  static final String DISTANCE_FROM_CHECKPOINT = "distanceFromCheckpoint";

  private final ValueProtos.L1.Builder builder = ValueProtos.L1.newBuilder();

  RocksL1() {
    super();
  }

  @Override
  public L1 commitMetadataId(Id id) {
    builder.setMetadataId(id.getValue());
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    builder
        .clearAncestors()
        .addAllAncestors(ids.map(Id::getValue).collect(Collectors.toList()));
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    builder
        .clearTree()
        .addAllTree(ids.map(Id::getValue).collect(Collectors.toList()));
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    builder
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
    builder.setIncrementalList(
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
    builder.setCompleteList(
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
            || !Id.of(builder.getMetadataId()).toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case ANCESTORS:
        evaluate(function, builder.getAncestorsList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case CHILDREN:
        evaluate(function, builder.getTreeList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case KEY_LIST:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
      case INCREMENTAL_KEY_LIST:
        if (!nameSegment.getChild().isPresent() || !function.getOperator().equals(Function.Operator.EQUALS)) {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        final String childName = nameSegment.getChild().get().asName().getName();
        if (childName.equals(CHECKPOINT_ID)) {
          if (!Id.of(builder.getIncrementalList().getCheckpointId()).toEntity().equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else if (childName.equals((DISTANCE_FROM_CHECKPOINT))) {
          if (!Entity.ofNumber(builder.getIncrementalList().getDistanceFromCheckpointId()).equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          // Invalid Condition Function.
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        break;
      case COMPLETE_KEY_LIST:
        evaluate(function, builder.getCompleteList().getFragmentIdsList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  protected void remove(String fieldName, int position) {
    switch (fieldName) {
      case ANCESTORS:
        List<ByteString> updatedAncestors = new ArrayList<>(l1Builder.getAncestorsList());
        updatedAncestors.remove(position);
        l1Builder.clearAncestors().addAllAncestors(updatedAncestors);
        break;
      case CHILDREN:
        List<ByteString> updatedChildren = new ArrayList<>(l1Builder.getTreeList());
        updatedChildren.remove(position);
        l1Builder.clearTree().addAllTree(updatedChildren);
        break;
      case KEY_LIST:
        throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
      case COMPLETE_KEY_LIST:
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.remove(position);
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName) {
    switch (fieldName) {
      case ANCESTORS:
      case CHILDREN:
      case KEY_LIST:
      case COMPLETE_KEY_LIST:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void appendToList(String fieldName, List<Entity> valuesToAdd) {
    switch (fieldName) {
      case ANCESTORS:
        l1Builder.addAllAncestors(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case CHILDREN:
        l1Builder.addAllTree(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case KEY_LIST:
        throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
      case COMPLETE_KEY_LIST:
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  @Override
  protected void appendToList(String fieldName, Entity valueToAdd) {
    switch (fieldName) {
      case ANCESTORS:
        l1Builder.addAncestors(valueToAdd.getBinary());
        break;
      case CHILDREN:
        l1Builder.addTree(valueToAdd.getBinary());
        break;
      case KEY_LIST:
        throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
      case COMPLETE_KEY_LIST:
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.add(valueToAdd.getBinary());
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  @Override
  protected void set(String fieldName, int position, Entity newValue) {
    switch (fieldName) {
      case ANCESTORS:
        l1Builder.setAncestors(position, newValue.getBinary());
        break;
      case CHILDREN:
        l1Builder.setTree(position, newValue.getBinary());
        break;
      case KEY_LIST:
        throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
      case COMPLETE_KEY_LIST:
        List<ByteString> updatedKeyList = new ArrayList<>(l1Builder.getCompleteList().getFragmentIdsList());
        updatedKeyList.set(position, newValue.getBinary());
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder().addAllFragmentIds(updatedKeyList));
        break;
      default:
        throw new UnsupportedOperationException(String.format("%s is not a list", fieldName));
    }
  }

  @Override
  protected void set(String fieldName, Entity newValue, Optional<ExpressionPath.PathSegment> childPath) {
    switch (fieldName) {
      case ANCESTORS:
        l1Builder.clearAncestors().addAllAncestors(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case CHILDREN:
        l1Builder.clearTree().addAllTree(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()));
        break;
      case KEY_LIST:
        throw new UnsupportedOperationException(String.format("Update not supported for %s", fieldName));
      case COMPLETE_KEY_LIST:
        l1Builder.setCompleteList(ValueProtos.CompleteList.newBuilder()
            .addAllFragmentIds(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()))
        );
        break;
      case INCREMENTAL_KEY_LIST:
        if (!childPath.isPresent() || !childPath.get().isName()) {
          throw new UnsupportedOperationException();
        }

        if (CHECKPOINT_ID.equals(childPath.get().asName().getName())) {
          l1Builder.setIncrementalList(ValueProtos.IncrementalList
              .newBuilder(l1Builder.getIncrementalList())
              .setCheckpointId(newValue.getBinary())
          );
        } else if (DISTANCE_FROM_CHECKPOINT.equals(childPath.get().asName().getName())) {
          l1Builder.setIncrementalList(ValueProtos.IncrementalList
              .newBuilder(l1Builder.getIncrementalList())
              .setDistanceFromCheckpointId((int)newValue.getNumber())
          );
        } else {
          throw new UnsupportedOperationException();
        }
        break;
      case COMMIT_METADATA:
        commitMetadataId(Id.of(newValue.getBinary()));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  byte[] build() {
    checkPresent(builder.getMetadataId(), COMMIT_METADATA);
    checkPresent(builder.getAncestorsList(), ANCESTORS);
    checkPresent(builder.getTreeList(), CHILDREN);
    checkPresent(builder.getKeyMutationsList(), KEY_LIST);

    if (builder.hasIncrementalList()) {
      checkPresent(builder.getIncrementalList().getCheckpointId(), CHECKPOINT_ID);
    } else {
      checkPresent(builder.getCompleteList().getFragmentIdsList(), COMPLETE_KEY_LIST);
    }

    return builder.setBase(buildBase()).build().toByteArray();
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
}
