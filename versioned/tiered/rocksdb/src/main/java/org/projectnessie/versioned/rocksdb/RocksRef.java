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
import java.util.Map;
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
  static final String COMMITS_ID = "id";
  static final String COMMITS_COMMIT = "commit";
  static final String COMMITS_PARENT = "parent";
  static final String COMMITS_DELTA = "deltas";
  static final String COMMITS_POSITION = "position";
  static final String COMMITS_OLD_ID = "old";
  static final String COMMITS_NEW_ID = "new";
  static final String COMMITS_KEY_LIST = "keys";
  static final String COMMITS_KEY_ADDITION = "a";
  static final String COMMITS_KEY_REMOVAL = "d";
  static final String COMMIT = "commit";
  static final String CHILDREN = "children";

  private final ValueProtos.Ref.Builder refBuilder = ValueProtos.Ref.newBuilder();

  RocksRef() {
    super();
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    if (refBuilder.getRefValueCase() == ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
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
            || refBuilder.getRefValueCase() != typeFromFunction) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case NAME:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !refBuilder.getName().equals(function.getValue().getString())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case CHILDREN:
        if (!refBuilder.hasBranch()) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        evaluate(function, refBuilder.getBranch().getChildrenList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      case METADATA:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !refBuilder.hasBranch()
            || !Id.of(refBuilder.getBranch().getMetadataId()).toEntity().equals(function.getValue())) {
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
          || !refBuilder.hasBranch()
          || refBuilder.getBranch().getCommitsCount() != function.getValue().getNumber()) {
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
        || !refBuilder.hasTag()
        || !Id.of(refBuilder.getTag().getId()).toEntity().equals(function.getValue())) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }
  }

  @Override
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    switch (fieldName) {
      case CHILDREN:
        if (!refBuilder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Remove \"%s\" is not supported for tags", fieldName));
        }

        List<ByteString> updatedChildren = new ArrayList<>(refBuilder.getBranch().getChildrenList());
        updatedChildren.remove(getPosition(path));
        refBuilder.setBranch(ValueProtos.Branch
            .newBuilder(refBuilder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren));
        break;
      case COMMITS:
        // TODO: implement removal of commits.delta and commits.key_mutations, but for now do not fail.
        if (!refBuilder.hasBranch() || path.getChild().isPresent()) {
          throw new UnsupportedOperationException(String.format("Remove \"%s\" is not supported for tags", fieldName));
        }

        List<ValueProtos.Commit> updatedCommits = new ArrayList<>(refBuilder.getBranch().getCommitsList());
        updatedCommits.remove(getPosition(path));
        refBuilder.setBranch(ValueProtos.Branch
            .newBuilder(refBuilder.getBranch())
            .clearCommits()
            .addAllCommits(updatedCommits));
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
        if (!refBuilder.hasBranch() || childPath != null) {
          throw new UnsupportedOperationException(String.format("Append to list \"%s\" not supported for tags", fieldName));
        }

        List<ByteString> updatedChildren = new ArrayList<>(refBuilder.getBranch().getChildrenList());
        updatedChildren.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        refBuilder.setBranch(ValueProtos.Branch
            .newBuilder(refBuilder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren));
        break;
      case COMMITS:
        if (!refBuilder.hasBranch() || childPath != null) {
          throw new UnsupportedOperationException(String.format("Append to list \"%s\" not supported for tags", fieldName));
        }

        List<ValueProtos.Commit> updatedCommits = new ArrayList<>(refBuilder.getBranch().getCommitsList());
        // TODO: this needs to convert a MapEntity into a Commit object
        updatedCommits.addAll(valuesToAdd.stream().map(e -> entityToCommit(e)).collect(Collectors.toList()));
        refBuilder.setBranch(ValueProtos.Branch
            .newBuilder(refBuilder.getBranch())
            .clearCommits()
            .addAllCommits(updatedCommits));
        break;
      default:
        throw new UnsupportedOperationException(String.format("\"%s\" is not a list", fieldName));
    }
  }

  /**
   * This converts a complex Entity type into an existing Protobuf object type.
   * @param entity the entity to convert
   * @return the object converted from the entity
   */
  static ValueProtos.Commit entityToCommit(Entity entity) {
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

  static ValueProtos.Delta entityToDelta(Entity entity) {
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

  static ValueProtos.KeyMutation entityToKeyMutation(Entity entity) {
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

  static ValueProtos.Key entityToKey(Entity entity) {
    if (!entity.getType().equals(Entity.EntityType.LIST)) {
      throw new UnsupportedOperationException();
    }

    return ValueProtos.Key.newBuilder().addAllElements(
        entity.getList().stream().map(Entity::getString).collect(Collectors.toList())).build();
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    switch (fieldName) {
      case NAME:
        if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        refBuilder.setName(newValue.getString());
        break;
      case CHILDREN:
        if (!refBuilder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", fieldName));
        }

        final List<ByteString> updatedChildren;
        if (childPath != null) {
          updatedChildren = new ArrayList<>(refBuilder.getBranch().getChildrenList());
          updatedChildren.set(getPosition(childPath), newValue.getBinary());
        } else {
          updatedChildren = newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList());
        }

        refBuilder.setBranch(ValueProtos.Branch
            .newBuilder(refBuilder.getBranch())
            .clearChildren()
            .addAllChildren(updatedChildren)
        );
        break;
      case METADATA:
        if (!refBuilder.hasBranch()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", fieldName));
        } else if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        refBuilder.setBranch(ValueProtos.Branch.newBuilder(refBuilder.getBranch()).setMetadataId(newValue.getBinary()));
        break;
      case COMMITS:
        setCommits(childPath, newValue);
        break;
      case COMMIT:
        if (!refBuilder.hasTag()) {
          throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for branches", fieldName));
        } else if (childPath != null) {
          throw new UnsupportedOperationException("Invalid path for SetEquals");
        }

        refBuilder.setTag(ValueProtos.Tag.newBuilder(refBuilder.getTag()).setId(newValue.getBinary()));
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unknown field \"%s\"", fieldName));
    }
  }

  // TODO: pass the fieldName in rather than calculate from the childPath, for consistency.
  private void setCommits(ExpressionPath.PathSegment childPath, Entity newValue) {
    if (!refBuilder.hasBranch() || childPath == null || !childPath.isPosition() || !childPath.getChild().isPresent()
        || !childPath.getChild().get().isName()) {
      throw new UnsupportedOperationException(String.format("Update not supported for %s", COMMITS));
    }

    final int commitsPosition = childPath.asPosition().getPosition();

    switch (childPath.getChild().get().asName().getName()) {
      case COMMITS_ID:
        refBuilder.setBranch(
            ValueProtos.Branch.newBuilder(refBuilder.getBranch())
              .setCommits(commitsPosition,
                ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(commitsPosition)).setId(newValue.getBinary())));
        break;
      case COMMITS_COMMIT:
        refBuilder.setBranch(
            ValueProtos.Branch.newBuilder(refBuilder.getBranch())
              .setCommits(commitsPosition,
                ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(commitsPosition)).setCommit(newValue.getBinary())));
        break;
      case COMMITS_PARENT:
        refBuilder.setBranch(
            ValueProtos.Branch.newBuilder(refBuilder.getBranch())
              .setCommits(commitsPosition,
                ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(commitsPosition)).setParent(newValue.getBinary())));
        break;
      case COMMITS_DELTA:
        setDelta(childPath.getChild().get(), commitsPosition, newValue);
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void setDelta(ExpressionPath.PathSegment childPath, int commitsPosition, Entity newValue) {
    if (!childPath.getChild().isPresent() || !childPath.getChild().get().isPosition()) {
      throw new UnsupportedOperationException();
    }

    final int deltaPosition = childPath.getChild().get().asPosition().getPosition();

    final ValueProtos.Delta.Builder deltaBuilder =
        ValueProtos.Delta.newBuilder(refBuilder.getBranch().getCommits(commitsPosition).getDeltaList().get(deltaPosition));

    switch (childPath.getChild().get().getChild().get().asName().getName()) {
      case COMMITS_POSITION:
        setDelta(commitsPosition, deltaPosition, deltaBuilder.setPosition((int)newValue.getNumber()));
        break;
      case COMMITS_OLD_ID:
        setDelta(commitsPosition, deltaPosition, deltaBuilder.setOldId(newValue.getBinary()));
        break;
      case COMMITS_NEW_ID:
        setDelta(commitsPosition, deltaPosition, deltaBuilder.setNewId(newValue.getBinary()));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  private void setDelta(int commitsPosition, int deltaPosition, ValueProtos.Delta.Builder deltaBuilder) {
    refBuilder.setBranch(
        ValueProtos.Branch.newBuilder(refBuilder.getBranch())
          .setCommits(commitsPosition,
            ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(commitsPosition))
              .setDelta(deltaPosition, deltaBuilder)));
  }

  @Override
  public Ref name(String name) {
    refBuilder.setName(name);
    return this;
  }

  @Override
  public Tag tag() {
    if (refBuilder.getRefValueCase() != ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }

    refBuilder.setTag(ValueProtos.Tag.newBuilder().build());
    return new RocksTag();
  }

  @Override
  public Branch branch() {
    if (refBuilder.getRefValueCase() != ValueProtos.Ref.RefValueCase.REFVALUE_NOT_SET) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }

    refBuilder.setBranch(ValueProtos.Branch.newBuilder().build());
    return new RocksBranch();
  }


  class RocksTag implements Tag {
    @Override
    public Tag commit(Id commit) {
      refBuilder.setTag(ValueProtos.Tag.newBuilder().setId(commit.getValue()).build());
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
      if (refBuilder.getRefValueCase() != ValueProtos.Ref.RefValueCase.BRANCH) {
        refBuilder.setBranch(ValueProtos.Branch.newBuilder().setMetadataId(metadata.getValue()));
      } else {
        refBuilder.setBranch(
            ValueProtos.Branch.newBuilder(refBuilder.getBranch()).setMetadataId(metadata.getValue())
        );
      }

      return this;
    }

    @Override
    public Branch children(Stream<Id> children) {
      final List<ByteString> childList = children.map(Id::getValue).collect(Collectors.toList());

      if (!refBuilder.hasBranch()) {
        refBuilder.setBranch(
            ValueProtos.Branch
                .newBuilder()
                .addAllChildren(childList)
        );
      } else {
        refBuilder.setBranch(
            ValueProtos.Branch
                .newBuilder(refBuilder.getBranch())
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
      refBuilder.setBranch(ValueProtos.Branch.newBuilder(refBuilder.getBranch()).addCommits(builder.build()));
      builder.clear();
      return this;
    }

    // UnsavedCommitDelta implementations
    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      builder.addDelta(ValueProtos.Delta.newBuilder()
          .setPosition(position)
          .setOldId(oldId.getValue())
          .setNewId(newId.getValue())
          .build());
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
    checkPresent(refBuilder.getName(), NAME);

    if (refBuilder.hasTag()) {
      checkPresent(refBuilder.getTag().getId(), COMMIT);
    } else {
      checkPresent(refBuilder.getBranch().getCommitsList(), COMMITS);
      checkPresent(refBuilder.getBranch().getChildrenList(), CHILDREN);
      checkPresent(refBuilder.getBranch().getMetadataId(), METADATA);
    }

    return refBuilder.setBase(buildBase()).build().toByteArray();
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
    return refBuilder.getName();
  }

  Stream<Id> getChildren() {
    if (refBuilder.hasBranch()) {
      return refBuilder.getBranch().getChildrenList().stream().map(Id::of);
    } else {
      return Stream.empty();
    }
  }

  Id getMetadata() {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getMetadataId());
    } else {
      return null;
    }
  }

  Id getCommit() {
    if (refBuilder.hasTag()) {
      return Id.of(refBuilder.getTag().getId());
    } else {
      return null;
    }
  }

  List<ValueProtos.Commit> getCommits() {
    return refBuilder.getBranch().getCommitsList();
  }

  Id getCommitsId(int index) {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getCommits(index).getId());
    } else {
      return null;
    }
  }

  Id getCommitsCommit(int index) {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getCommits(index).getCommit());
    } else {
      return null;
    }
  }

  Id getCommitsParent(int index) {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getCommits(index).getParent());
    } else {
      return null;
    }
  }

  Integer getCommitsDeltaPosition(int commitsPosition, int deltaPosition) {
    if (refBuilder.hasBranch()) {
      return refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition).getPosition();
    } else {
      return null;
    }
  }

  Id getCommitsDeltaOldId(int commitsPosition, int deltaPosition) {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition).getOldId());
    } else {
      return null;
    }
  }

  Id getCommitsDeltaNewId(int commitsPosition, int deltaPosition) {
    if (refBuilder.hasBranch()) {
      return Id.of(refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition).getNewId());
    } else {
      return null;
    }
  }
}
