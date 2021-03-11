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
 *
 * <p>Conceptually, this is matching one of the following JSON structures:</p>
 * <pre>{
 *   "id": &lt;ByteString&gt;, // ID
 *   "dt": &lt;int64&gt;,      // DATETIME
 *   "name": &lt;String&gt;,   // NAME
 *   "id": &lt;ByteString&gt;  // COMMIT
 * }
 *
 * {
 *   "id": &lt;ByteString&gt;,           // ID
 *   "dt": &lt;int64&gt;,                // DATETIME
 *   "commits": [                        // COMMITS
 *     {
 *       "id": &lt;ByteString&gt;,       // COMMITS_ID
 *       "commit": &lt;ByteString&gt;,   // COMMITS_COMMIT
 *       "parent": &lt;ByteString&gt;,   // COMMITS_PARENT
 *       "deltas": [                     // COMMITS_DELTA
 *         {
 *           "position": &lt;int32&gt;,  // COMMITS_POSITION
 *           "old": &lt;ByteString&gt;,  // COMMITS_OLD_ID
 *           "new": &lt;ByteString&gt;,  // COMMITS_NEW_ID
 *         }
 *       ],
 *       "keys": [                       // COMMITS_KEY_LIST
 *         {
 *           "mutationType": "a" or "b", // COMMITS_KEY_ADDITION or COMMITS_KEY_REMOVAL
 *           "key": [
 *             &lt;String&gt;
 *           ]
 *         }
 *       ]
 *     }
 *   ],
 *   "children": [                       // CHILDREN
 *     &lt;ByteString&gt;
 *   ],
 *   "metadata": &lt;ByteString&gt;      // METADATA
 * }</pre>
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
  static final String COMMITS_KEY_LIST_KEY = "key";
  static final String COMMITS_KEY_ADDITION = "a";
  static final String COMMITS_KEY_REMOVAL = "d";
  static final String COMMIT = "commit";
  static final String CHILDREN = "tree";

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
    if (!refBuilder.hasBranch()) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }

    switch (function.getOperator()) {
      case SIZE:
        if (function.getRootPathAsNameSegment().getChild().isPresent()
            || refBuilder.getBranch().getCommitsCount() != function.getValue().getNumber()) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      case EQUALS:
        if (function.getPath().accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_ID))) {
          final int i = function.getPath().getRoot().getChild().get().asPosition().getPosition();
          if (refBuilder.getBranch().getCommitsCount() <= i
              || !function.getValue().getBinary().equals(refBuilder.getBranch().getCommits(i).getId())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      default:
        throw new ConditionFailedException(conditionNotMatchedMessage(function));
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
  protected void remove(ExpressionPath path) {
    // tree[*]
    if (path.accept(PathPattern.exact(CHILDREN).anyPosition())) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Remove \"%s\" is not supported for tags", path.asString()));
      }

      List<ByteString> updatedChildren = new ArrayList<>(refBuilder.getBranch().getChildrenList());
      updatedChildren.remove(getPathSegmentAsPosition(path, 1));
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .clearChildren()
          .addAllChildren(updatedChildren));
    // commits[*]
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition())) {
      List<ValueProtos.Commit> updatedCommits = new ArrayList<>(refBuilder.getBranch().getCommitsList());
      updatedCommits.remove(getPathSegmentAsPosition(path, 1));
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .clearCommits()
          .addAllCommits(updatedCommits));
    // commits[*]/deltas
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_DELTA))) {
      final int i = getPathSegmentAsPosition(path, 1);
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(i, ValueProtos.Commit
            .newBuilder(refBuilder.getBranch().getCommits(i))
            .clearDelta()));
    // commits[*]/deltas[*]
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_DELTA).anyPosition())) {
      final int commitIndex = getPathSegmentAsPosition(path, 1);
      final int deltaIndex = getPathSegmentAsPosition(path, 3);

      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(commitIndex, ValueProtos.Commit
            .newBuilder(refBuilder.getBranch().getCommits(commitIndex))
            .removeDelta(deltaIndex)));
    // commits[*]/keys
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_KEY_LIST))) {
      final int i = getPathSegmentAsPosition(path, 1);
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(i, ValueProtos.Commit
            .newBuilder(refBuilder.getBranch().getCommits(i))
            .clearKeyMutation()));
    // commits[*]/keys[*]
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_KEY_LIST).anyPosition())) {
      final int commitIndex = getPathSegmentAsPosition(path, 1);
      final int keyMutationIndex = getPathSegmentAsPosition(path, 3);

      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(commitIndex, ValueProtos.Commit
            .newBuilder(refBuilder.getBranch().getCommits(commitIndex))
            .removeKeyMutation(keyMutationIndex)));
    // commits[*]/keys[*]/key[*]
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_KEY_LIST)
        .anyPosition().nameEquals(COMMITS_KEY_LIST_KEY).anyPosition())) {
      final int commitIndex = getPathSegmentAsPosition(path, 1);
      final int keyMutationIndex = getPathSegmentAsPosition(path, 3);
      final int keyIndex = getPathSegmentAsPosition(path, 5);

      final List<String> updateKeys = new ArrayList<>(refBuilder.getBranch().getCommits(commitIndex)
          .getKeyMutation(keyMutationIndex).getKey().getElementsList());
      updateKeys.remove(keyIndex);

      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(commitIndex, ValueProtos.Commit
            .newBuilder(refBuilder.getBranch().getCommits(commitIndex))
            .setKeyMutation(keyMutationIndex, ValueProtos.KeyMutation
              .newBuilder(refBuilder.getBranch().getCommits(commitIndex).getKeyMutation(keyMutationIndex))
              .setKey(ValueProtos.Key.newBuilder().addAllElements(updateKeys)))));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for remove in Ref", path.asString()));
    }
  }

  @Override
  protected boolean fieldIsList(ExpressionPath path) {
    return path.accept(PathPattern.exact(CHILDREN))
        || path.accept(PathPattern.exact(COMMITS))
        || path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_DELTA))
        || path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_KEY_LIST))
        || path.accept(PathPattern.exact(COMMITS).anyPosition()
            .nameEquals(COMMITS_KEY_LIST).anyPosition().nameEquals(COMMITS_KEY_LIST_KEY));
  }

  @Override
  protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
    if (path.accept(PathPattern.exact(CHILDREN))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Append to list \"%s\" not supported for tags", CHILDREN));
      }

      final ValueProtos.Branch.Builder branchBuilder = ValueProtos.Branch.newBuilder(refBuilder.getBranch());
      valuesToAdd.forEach(e -> branchBuilder.addChildren(e.getBinary()));
      refBuilder.setBranch(branchBuilder);
    } else if (path.accept(PathPattern.exact(COMMITS))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Append to list \"%s\" not supported for tags", COMMITS));
      }

      final ValueProtos.Branch.Builder branchBuilder = ValueProtos.Branch.newBuilder(refBuilder.getBranch());
      valuesToAdd.forEach(e -> branchBuilder.addCommits(EntityConverter.entityToCommit(e)));
      refBuilder.setBranch(branchBuilder);
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for append in L1", path.asString()));
    }
  }

  @Override
  protected void set(ExpressionPath path, Entity newValue) {
    if (path.accept(PathPattern.exact(NAME))) {
      refBuilder.setName(newValue.getString());
    } else if (path.accept(PathPattern.exact(CHILDREN))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final ValueProtos.Branch.Builder branchBuilder = ValueProtos.Branch.newBuilder(refBuilder.getBranch()).clearChildren();
      newValue.getList().forEach(e -> branchBuilder.addChildren(e.getBinary()));
      refBuilder.setBranch(branchBuilder);
    } else if (path.accept(PathPattern.exact(CHILDREN).anyPosition())) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int i = getPathSegmentAsPosition(path, 1);
      final ValueProtos.Branch.Builder branchBuilder = ValueProtos.Branch.newBuilder(refBuilder.getBranch());
      branchBuilder.setChildren(i, newValue.getBinary());
      refBuilder.setBranch(branchBuilder);
    } else if (path.accept(PathPattern.exact(METADATA))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      refBuilder.setBranch(ValueProtos.Branch.newBuilder(refBuilder.getBranch()).setMetadataId(newValue.getBinary()));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_ID))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int i = getPathSegmentAsPosition(path, 1);
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(i, ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(i)).setId(newValue.getBinary())));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_COMMIT))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int i = getPathSegmentAsPosition(path, 1);
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(i, ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(i)).setCommit(newValue.getBinary())));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition().nameEquals(COMMITS_PARENT))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int i = getPathSegmentAsPosition(path, 1);
      refBuilder.setBranch(ValueProtos.Branch
          .newBuilder(refBuilder.getBranch())
          .setCommits(i, ValueProtos.Commit.newBuilder(refBuilder.getBranch().getCommits(i)).setParent(newValue.getBinary())));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition()
        .nameEquals(COMMITS_DELTA).anyPosition().nameEquals(COMMITS_POSITION))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int commitsPosition = getPathSegmentAsPosition(path, 1);
      final int deltaPosition = getPathSegmentAsPosition(path, 3);
      final ValueProtos.Delta.Builder deltaBuilder = ValueProtos.Delta
          .newBuilder(refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition));

      setDelta(commitsPosition, deltaPosition, deltaBuilder.setPosition((int)newValue.getNumber()));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition()
        .nameEquals(COMMITS_DELTA).anyPosition().nameEquals(COMMITS_OLD_ID))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int commitsPosition = getPathSegmentAsPosition(path, 1);
      final int deltaPosition = getPathSegmentAsPosition(path, 3);
      final ValueProtos.Delta.Builder deltaBuilder = ValueProtos.Delta
          .newBuilder(refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition));

      setDelta(commitsPosition, deltaPosition, deltaBuilder.setOldId(newValue.getBinary()));
    } else if (path.accept(PathPattern.exact(COMMITS).anyPosition()
        .nameEquals(COMMITS_DELTA).anyPosition().nameEquals(COMMITS_NEW_ID))) {
      if (!refBuilder.hasBranch()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for tags", path.asString()));
      }

      final int commitsPosition = getPathSegmentAsPosition(path, 1);
      final int deltaPosition = getPathSegmentAsPosition(path, 3);
      final ValueProtos.Delta.Builder deltaBuilder = ValueProtos.Delta
          .newBuilder(refBuilder.getBranch().getCommits(commitsPosition).getDelta(deltaPosition));

      setDelta(commitsPosition, deltaPosition, deltaBuilder.setNewId(newValue.getBinary()));
    } else if (path.accept(PathPattern.exact(COMMIT))) {
      if (!refBuilder.hasTag()) {
        throw new UnsupportedOperationException(String.format("Cannot set \"%s\" for branches", path.asString()));
      }

      refBuilder.setTag(ValueProtos.Tag.newBuilder(refBuilder.getTag()).setId(newValue.getBinary()));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for set equals in Ref", path.asString()));
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
      final ValueProtos.Branch.Builder branchBuilder;
      if (!refBuilder.hasBranch()) {
        branchBuilder = ValueProtos.Branch.newBuilder();
      } else {
        branchBuilder = ValueProtos.Branch.newBuilder(refBuilder.getBranch());
        branchBuilder.clearChildren();
      }

      children.forEach(id -> branchBuilder.addChildren(id.getValue()));
      refBuilder.setBranch(branchBuilder);

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
