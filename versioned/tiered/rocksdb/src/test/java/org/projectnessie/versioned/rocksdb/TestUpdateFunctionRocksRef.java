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
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
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
  private static final String sampleName = createString(random, 10);
  static final Id ID_3 = createId(new Random(getRandomSeed()));
  static final Id ID_4 = createId(new Random(getRandomSeed()));

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
          .keyMutation(Key.of(createString(random, 8), createString(random, 8)).asAddition())
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

  /**
   * Create a Sample ID entity.
   * @param random object to use for randomization of entity creation.
   * @return sample ID entity.
   */
  static Id createId(Random random) {
    return Id.of(createBinary(random, 20));
  }

  /**
   * Create an array of random bytes.
   * @param random random number generator to use.
   * @param numBytes the size of the array.
   * @return the array of random bytes.
   */
  static byte[] createBinary(Random random, int numBytes) {
    final byte[] buffer = new byte[numBytes];
    random.nextBytes(buffer);
    return buffer;
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
  void childrenUpdateWhenTag() {
    final List<Id> newChildren = Lists.newArrayList(SampleEntities.createId(RANDOM), SampleEntities.createId(RANDOM));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(
            ExpressionPath.builder(RocksRef.CHILDREN).build(),
            Entity.ofList(newChildren.stream().map(Id::toEntity))));
    updateTestFails(rocksRefTag, updateExpression);
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

  @Test
  void commitSetEqualsWhenBranch() {
    final Id newId = SampleEntities.createId(RANDOM);
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksRef.COMMIT).build(), newId.toEntity()));
    updateTestFails(rocksRefBranch, updateExpression);
  }
}
