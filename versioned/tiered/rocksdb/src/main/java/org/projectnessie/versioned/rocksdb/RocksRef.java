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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.Ref;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.Ref} providing
 * SerDe and Condition evaluation.
 */
class RocksRef extends RocksBaseValue<Ref> implements Ref {

  static final String TYPE = "type";
  static final String NAME = "name";
  static final String METADATA = "metadata";
  static final String COMMITS = "commits";
  static final String COMMIT = "commit";
  static final String CHILDREN = "children";

  private final ValueProtos.Ref.Builder builder = ValueProtos.Ref.newBuilder();

  RocksRef() {
    super();
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    if (builder.getRefValueCase() == ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
      throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }

    final String segment = function.getRootPathAsNameSegment().getName();

    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case TYPE:
        final ValueProtos.Ref.RefValueCase typeFromFunction;
        if (function.getValue().getString().equals("b")) {
          typeFromFunction = ValueProtos.Ref.RefValueCase.BRANCH;
        } else if (function.getValue().getString().equals("t")) {
          typeFromFunction = ValueProtos.Ref.RefValueCase.TAG;
        } else {
          throw new IllegalArgumentException(String.format("Unknown type name [%s].", function.getValue().getString()));
        }

        if (!function.isRootNameSegmentChildlessAndEquals()
            || builder.getRefValueCase() != typeFromFunction) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case NAME:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !builder.getName().equals(function.getValue().getString())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case CHILDREN:
        if (!builder.hasBranch()) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        evaluate(function, builder.getBranch().getChildrenList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case METADATA:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !builder.hasBranch()
            || !Id.of(builder.getBranch().getMetadataId()).toEntity().equals(function.getValue())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case COMMIT:
        evaluateTagCommit(function);
        break;
      case COMMITS:
        evaluateBranchCommits(function);
        break;
      default:
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  /**
   * Evaluates that this branch meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  private void evaluateBranchCommits(Function function) {
    // TODO: refactor once jdbc-store Store changes are available.
    if (function.getOperator().equals(Function.Operator.SIZE)) {
      if (function.getRootPathAsNameSegment().getChild().isPresent()
          || !builder.hasBranch()
          || builder.getBranch().getCommitsCount() != function.getValue().getNumber()) {
        throw new ConditionFailedException(conditionNotMatchedMessage(function));
      }
    } else {
      throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  /**
   * Evaluates that this tag meets the condition.
   *
   * @param function the function that is tested against the nameSegment
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  private void evaluateTagCommit(Function function) {
    if (!function.getOperator().equals(Function.Operator.EQUALS)
        || !builder.hasTag()
        || !Id.of(builder.getTag().getId()).toEntity().equals(function.getValue())) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }
  }

  @Override
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    switch (fieldName) {
      case CHILDREN:
        if (!builder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Remove \"%s\" is not supported for tags", fieldName));
        }

        List<ByteString> updatedChildren = new ArrayList<>(builder.getBranch().getChildrenList());
        updatedChildren.remove(getPosition(path));
        builder.setBranch(ValueProtos.Branch
            .newBuilder(builder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren));
        break;
      default:
        throw new UnsupportedOperationException(String.format("Remove not supported for \"%s\"", fieldName));
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    switch (fieldName) {
      case CHILDREN:
      case COMMITS:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    switch (fieldName) {
      case CHILDREN:
        if (!builder.hasBranch() || childPath != null) {
          throw new UnsupportedOperationException(String.format("Append to list \"%s\" not supported for tags", fieldName));
        }

        List<ByteString> updatedChildren = new ArrayList<>(builder.getBranch().getChildrenList());
        updatedChildren.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        builder.setBranch(ValueProtos.Branch
            .newBuilder(builder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren));
        break;
      default:
        throw new UnsupportedOperationException(String.format("\"%s\" is not a list", fieldName));
    }
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    switch (fieldName) {
      case NAME:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        builder.setName(newValue.getString());
        break;
      case CHILDREN:
        if (!builder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", fieldName));
        }

        final List<ByteString> updatedChildren;
        if (childPath != null) {
          updatedChildren = new ArrayList<>(builder.getBranch().getChildrenList());
          updatedChildren.set(getPosition(childPath), newValue.getBinary());
        } else {
          updatedChildren = newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList());
        }

        builder.setBranch(ValueProtos.Branch
            .newBuilder(builder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren)
        );
        break;
      case METADATA:
        if (!builder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", fieldName));
        } else if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        builder.setBranch(ValueProtos.Branch.newBuilder(builder.getBranch()).setMetadataId(newValue.getBinary()));
        break;
      case COMMIT:
        if (!builder.hasTag()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for branches", fieldName));
        } else if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        builder.setTag(ValueProtos.Tag.newBuilder(builder.getTag()).setId(newValue.getBinary()));
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unknown field \"%s\"", fieldName));
    }
  }

  @Override
  public Ref name(String name) {
    builder.setName(name);
    return this;
  }

  @Override
  public Tag tag() {
    if (builder.getRefValueCase() != ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }

    builder.setTag(ValueProtos.Tag.newBuilder().build());
    return new RocksTag();
  }

  @Override
  public Branch branch() {
    if (builder.getRefValueCase() != ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }

    builder.setBranch(ValueProtos.Branch.newBuilder().build());
    return new RocksBranch();
  }


  class RocksTag implements Tag {
    @Override
    public Tag commit(Id commit) {
      builder.setTag(ValueProtos.Tag.newBuilder().setId(commit.getValue()).build());
      return this;
    }

    @Override
    public Ref backToRef() {
      return RocksRef.this;
    }
  }

  class RocksBranch implements Branch {
    @Override
    public Branch metadata(Id metadata) {
      if (builder.getRefValueCase() != ValueProtos.Ref.RefValueCase.BRANCH) {
        builder.setBranch(ValueProtos.Branch.newBuilder().setMetadataId(metadata.getValue()));
      } else {
        builder.setBranch(
            ValueProtos.Branch.newBuilder(builder.getBranch()).setMetadataId(metadata.getValue())
        );
      }

      return this;
    }

    @Override
    public Branch children(Stream<Id> children) {
      final List<ByteString> childList = children.map(Id::getValue).collect(Collectors.toList());

      if (!builder.hasBranch()) {
        builder.setBranch(
            ValueProtos.Branch
                .newBuilder()
                .addAllChildren(childList)
        );
      } else {
        builder.setBranch(
            ValueProtos.Branch
                .newBuilder(builder.getBranch())
                .clearChildren()
                .addAllChildren(childList)
        );
      }

      return this;
    }

    @Override
    public Branch commits(Consumer<BranchCommit> commitsConsumer) {
      commitsConsumer.accept(new RocksBranchCommit());
      return this;
    }

    @Override
    public Ref backToRef() {
      return RocksRef.this;
    }
  }

  private class RocksBranchCommit implements BranchCommit, SavedCommit, UnsavedCommitDelta, UnsavedCommitMutations {
    final ValueProtos.Commit.Builder builder = ValueProtos.Commit.newBuilder();

    // BranchCommit implementations
    @Override
    public BranchCommit id(Id id) {
      builder.setId(id.getValue());
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      builder.setCommit(commit.getValue());
      return this;
    }

    @Override
    public SavedCommit saved() {
      return this;
    }

    @Override
    public UnsavedCommitDelta unsaved() {
      return this;
    }

    // SavedCommit implementations
    @Override
    public SavedCommit parent(Id parent) {
      builder.setParent(parent.getValue());
      return this;
    }

    @Override
    public BranchCommit done() {
      RocksRef.this.builder.setBranch(ValueProtos.Branch.newBuilder(RocksRef.this.builder.getBranch()).addCommits(builder.build()));
      builder.clear();
      return this;
    }

    // UnsavedCommitDelta implementations
    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      builder.addDelta(ValueProtos.Delta.newBuilder()
          .setPosition(position)
          .setOldId(oldId.getValue())
          .setNewId(newId.getValue()));
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return this;
    }

    // UnsavedCommitMutations implementations
    @Override
    public UnsavedCommitMutations keyMutation(Key.Mutation keyMutation) {
      builder.addKeyMutation(buildKeyMutation(keyMutation));
      return this;
    }
  }

  @Override
  byte[] build() {
    checkPresent(builder.getName(), NAME);

    if (builder.hasTag()) {
      checkPresent(builder.getTag().getId(), COMMIT);
    } else {
      checkPresent(builder.getBranch().getCommitsList(), COMMITS);
      checkPresent(builder.getBranch().getChildrenList(), CHILDREN);
      checkPresent(builder.getBranch().getMetadataId(), METADATA);
    }

    return builder.setBase(buildBase()).build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, Ref consumer) {
    try {
      final ValueProtos.Ref ref = ValueProtos.Ref.parseFrom(value);
      setBase(consumer, ref.getBase());
      consumer.name(ref.getName());
      if (ref.hasTag()) {
        consumer.tag().commit(Id.of(ref.getTag().getId()));
      } else {
        // Branch
        consumer
            .branch()
            .commits(bc -> deserializeCommits(bc, ref.getBranch().getCommitsList()))
            .children(ref.getBranch().getChildrenList().stream().map(Id::of))
            .metadata(Id.of(ref.getBranch().getMetadataId()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Ref value encountered when deserializing.", e);
    }
  }

  private static void deserializeCommits(BranchCommit consumer, List<ValueProtos.Commit> commitsList) {
    for (ValueProtos.Commit commit : commitsList) {
      consumer
          .id(Id.of(commit.getId()))
          .commit(Id.of(commit.getCommit()));

      if (commit.getParent().isEmpty()) {
        final UnsavedCommitDelta unsaved = consumer.unsaved();
        commit.getDeltaList().forEach(d -> unsaved.delta(d.getPosition(), Id.of(d.getOldId()), Id.of(d.getNewId())));
        final UnsavedCommitMutations mutations = unsaved.mutations();
        commit.getKeyMutationList().forEach(km -> mutations.keyMutation(createKeyMutation(km)));
        mutations.done();
      } else {
        consumer.saved().parent(Id.of(commit.getParent())).done();
      }
    }
  }

  String getName() {
    return builder.getName();
  }

  Stream<Id> getChildren() {
    if (builder.hasBranch()) {
      return builder.getBranch().getChildrenList().stream().map(Id::of);
    } else {
      return Stream.empty();
    }
  }

  Id getMetadata() {
    if (builder.hasBranch()) {
      return Id.of(builder.getBranch().getMetadataId());
    } else {
      return null;
    }
  }

  Id getCommit() {
    if (builder.hasTag()) {
      return Id.of(builder.getTag().getId());
    } else {
      return null;
    }
  }
}
