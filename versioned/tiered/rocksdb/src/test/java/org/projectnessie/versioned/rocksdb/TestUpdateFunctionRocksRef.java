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

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

import com.google.common.collect.Lists;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksRef update() tests")
public class TestUpdateFunctionRocksRef extends TestUpdateFunctionBase {
  protected static final Random random = new Random(getRandomSeed());
  private static final String sampleName = SampleEntities.createString(random, 10);
  static final Id ID_3 = SampleEntities.createId(new Random(getRandomSeed()));
  static final Id ID_4 = SampleEntities.createId(new Random(getRandomSeed()));

  final RocksRef rocksRefBranch = createRefBranch(RANDOM);
  final RocksRef rocksRefTag = createRefTag(RANDOM);

  /**
   * Create a Sample Ref entity for a branch.
   * @param random object to use for randomization of entity creation.
   * @return sample Ref entity.
   */
  static RocksRef createRefBranch(Random random) {
    return (RocksRef) new RocksRef()
      .id(Id.EMPTY)
      .name(sampleName)
      .branch()
      .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
      .metadata(ID)
      .commits(bc -> {
        bc.id(ID)
          .commit(ID_2)
          .saved()
          .parent(ID_3)
          .done();
        bc.id(ID_4)
          .commit(ID)
          .unsaved()
          .delta(1, ID_2, ID_3)
          .mutations()
          .keyMutation(Key.of(SampleEntities.createString(random, 8), SampleEntities.createString(random, 8)).asAddition())
            .done();
      })
      .backToRef();
  }

  /**
   * Create a Sample Ref entity for a tag.
   * @param random object to use for randomization of entity creation.
   * @return sample Ref entity.
   */
  static RocksRef createRefTag(Random random) {
    return (RocksRef) new RocksRef()
      .id(Id.EMPTY)
      .name(sampleName)
      .tag()
      .commit(ID_2)
      .backToRef();
  }

  @Test
  void idRemove() {
    idRemove(rocksRefBranch);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksRefBranch);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksRefBranch);
  }

  @Nested
  @DisplayName("update() branch tests")
  class BranchTests {

    @Test
    void nameRemove() {
      final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.NAME).build()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void nameSetEquals() {
      final String newName = "foo";
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.NAME).build(), Entity.ofString(newName)));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(newName, rocksRefBranch.getName());
    }

    @Test
    void nameAppendToList() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.NAME).build(), Entity.ofString("foo")));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void childrenRemoveFirst() {
      final List<Id> expectedChildrenList = rocksRefBranch.getChildren().skip(1).collect(Collectors.toList());
      final UpdateExpression updateExpression =
          UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.CHILDREN).position(0).build()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(expectedChildrenList, rocksRefBranch.getChildren().collect(Collectors.toList()));
    }

    @Test
    void childrenRemove() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.CHILDREN).build()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void childrenSetEqualsWithScalar() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.CHILDREN).build(), newId.toEntity()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void childrenSetEqualsWithList() {
      final List<Id> newChildren = Lists.newArrayList(SampleEntities.createId(RANDOM), SampleEntities.createId(RANDOM));
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(
            ExpressionPath.builder(RocksRef.CHILDREN).build(),
            Entity.ofList(newChildren.stream().map(Id::toEntity))));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(newChildren, rocksRefBranch.getChildren().collect(Collectors.toList()));
    }

    @Test
    void childrenSetEqualsReplaceItem() {
      final Id newId = SampleEntities.createId(RANDOM);
      final List<Id> expectedChildren = rocksRefBranch.getChildren().collect(Collectors.toList());
      expectedChildren.set(0, newId);

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.CHILDREN).position(0).build(), newId.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(expectedChildren, rocksRefBranch.getChildren().collect(Collectors.toList()));
    }

    @Test
    void childrenAppendToListScalar() {
      final Id newId = SampleEntities.createId(RANDOM);
      final List<Id> expectedChildren = rocksRefBranch.getChildren().collect(Collectors.toList());
      expectedChildren.add(newId);

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.CHILDREN).build(), newId.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(expectedChildren, rocksRefBranch.getChildren().collect(Collectors.toList()));
    }

    @Test
    void childrenAppendToListWithList() {
      final List<Id> newChildren = Lists.newArrayList(SampleEntities.createId(RANDOM), SampleEntities.createId(RANDOM));
      final List<Id> expectedChildren = rocksRefBranch.getChildren().collect(Collectors.toList());
      expectedChildren.addAll(newChildren);

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(
            ExpressionPath.builder(RocksRef.CHILDREN).build(),
            Entity.ofList(newChildren.stream().map(Id::toEntity))));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(expectedChildren, rocksRefBranch.getChildren().collect(Collectors.toList()));
    }

    @Test
    void metadataRemove() {
      final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.METADATA).build()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void metadataSetEquals() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.METADATA).build(), newId.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(newId, rocksRefBranch.getMetadata());
    }

    @Test
    void metadataAppendToList() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.METADATA).build(), newId.toEntity()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void commitSetEqualsWhenBranch() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMIT).build(), newId.toEntity()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    @Test
    void commitsIdRemove() {
      commitsRemoveFail(RocksRef.ID);
    }

    @Test
    void commitsIdSetEquals() {
      final Id NEW_ID = SampleEntities.createId(new Random(getRandomSeed()));
      final int position = 0;

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMITS)
            .position(position)
            .name(RocksRef.ID)
            .build(), NEW_ID.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(NEW_ID, rocksRefBranch.getCommitsId(position));
    }

    @Test
    void commitsIdAppendToList() {
      commitsAppendToListFail(RocksRef.ID);
    }

    @Test
    void commitsCommitRemove() {
      commitsRemoveFail(RocksRef.COMMITS_COMMIT);
    }

    @Test
    void commitsCommitSetEquals() {
      final Id NEW_ID = SampleEntities.createId(new Random(getRandomSeed()));
      final int position = 0;

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMITS)
            .position(position)
            .name(RocksRef.COMMITS_COMMIT)
            .build(), NEW_ID.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(NEW_ID, rocksRefBranch.getCommitsId(position));
    }

    @Test
    void commitsCommitAppendToList() {
      commitsAppendToListFail(RocksRef.COMMITS_COMMIT);
    }

    @Test
    void commitsParentRemove() {
      commitsRemoveFail(RocksRef.COMMITS_PARENT);
    }

    @Test
    void commitsParentSetEquals() {
      final Id NEW_ID = SampleEntities.createId(new Random(getRandomSeed()));
      final int position = 0;

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMITS)
            .position(position)
            .name(RocksRef.COMMITS_PARENT)
            .build(), NEW_ID.toEntity()));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(NEW_ID, rocksRefBranch.getCommitsParent(position));
    }

    @Test
    void commitsParentAppendToList() {
      commitsAppendToListFail(RocksRef.COMMITS_COMMIT);
    }

    @Test
    void commitsDeltaPositionRemove() {
      commitsDeltaRemoveFail(RocksRef.COMMITS_POSITION);
    }

    @Test
    void commitsDeltaPositionSetEquals() {
      final int newValue = 5;
      final int position = 0;

      final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMITS)
          .position(position)
          .name(RocksRef.COMMITS_DELTA)
          .position(0)
          .name(RocksRef.COMMITS_POSITION)
          .build(), Entity.ofNumber(newValue)));
      rocksRefBranch.update(updateExpression);
      Assertions.assertEquals(newValue, rocksRefBranch.getCommitsParent(position));
    }

    @Test
    void commitsDeltaPositionAppendToList() {
      commitsDeltaAppendToListFail(RocksRef.COMMITS_POSITION, Entity.ofNumber(1));
    }

    private void commitsRemoveFail(String name) {
      final UpdateExpression updateExpression =
          UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.COMMITS)
            .position(0)
            .name(name)
            .build()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    private void commitsAppendToListFail(String name) {
      final Id NEW_ID = SampleEntities.createId(new Random(getRandomSeed()));

      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.COMMITS)
            .position(1)
            .name(name)
            .build(), NEW_ID.toEntity()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    private void commitsDeltaRemoveFail(String name) {
      final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.COMMITS)
          .position(0)
          .name(RocksRef.COMMITS_DELTA)
          .position(0)
          .name(name)
          .build()));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    private void commitsDeltaAppendToListFail(String name, Entity entityToAppend) {
      final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.COMMITS)
          .position(1)
          .name(RocksRef.COMMITS_DELTA)
          .position(0)
          .name(name)
          .build(), entityToAppend));
      updateTestFails(rocksRefBranch, updateExpression);
    }

    private void commitsDeltaAppendToListFail(String name) {
      final Id NEW_ID = SampleEntities.createId(new Random(getRandomSeed()));

      commitsDeltaAppendToListFail(name, NEW_ID.toEntity());
    }
  }

  @Nested
  @DisplayName("update() tag tests")
  class TagTests {

    @Test
    void childrenUpdateWhenTag() {
      final List<Id> newChildren = Lists.newArrayList(SampleEntities.createId(RANDOM), SampleEntities.createId(RANDOM));
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(
            ExpressionPath.builder(RocksRef.CHILDREN).build(),
            Entity.ofList(newChildren.stream().map(Id::toEntity))));
      updateTestFails(rocksRefTag, updateExpression);
    }

    @Test
    void metadataSetEqualsWhenTag() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.METADATA).build(), newId.toEntity()));
      updateTestFails(rocksRefTag, updateExpression);
    }

    @Test
    void commitRemove() {
      final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksRef.COMMIT).build()));
      updateTestFails(rocksRefTag, updateExpression);
    }

    @Test
    void commitSetEquals() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMIT).build(), newId.toEntity()));
      rocksRefTag.update(updateExpression);
      Assertions.assertEquals(newId, rocksRefTag.getCommit());
    }

    @Test
    void commitAppendToList() {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksRef.COMMIT).build(), newId.toEntity()));
      updateTestFails(rocksRefTag, updateExpression);
    }
  }
}
