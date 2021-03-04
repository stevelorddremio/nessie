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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
@DisplayName("RocksL1 update() tests")
public class TestUpdateFunctionRocksL1 extends TestUpdateFunctionBase {
  private RocksL1 rocksL1;

  /**
   * Create a Sample L1 entity.
   */
  @BeforeEach
  void createL1() {
    rocksL1 = (RocksL1) new RocksL1()
      .id(Id.EMPTY)
      .commitMetadataId(ID)
      .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
      .ancestors(Stream.generate(() -> ID).limit(RocksL1.SIZE))
      .keyMutations(Stream.of(Key.of(SampleEntities.createString(RANDOM, 8), SampleEntities.createString(RANDOM, 9)).asAddition()))
      .incrementalKeyList(ID, 1);
  }

  @Test
  void idRemove() {
    idRemove(rocksL1);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksL1);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksL1);
  }

  @Test
  void commitMetadataIdRemove() {
    final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.ID).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void commitMetadataIdSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.COMMIT_METADATA).build(), ID_2.toEntity()));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(ID_2, rocksL1.getMetadataId());
  }

  @Test
  void idCommitMetadataSetAppendToList() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.COMMIT_METADATA).build(), ID_2.toEntity()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void incrementalKeyListCheckpointIdRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
        RemoveClause.of(ExpressionPath.builder(RocksL1.CHECKPOINT_ID).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void incrementalKeyListRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
        RemoveClause.of(ExpressionPath.builder(RocksL1.DISTANCE_FROM_CHECKPOINT).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void incrementalKeyListDistanceFromCheckpointSetEquals() {
    final Long distanceFromCheckpoint = 32L;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.DISTANCE_FROM_CHECKPOINT)
        .build(), Entity.ofNumber(distanceFromCheckpoint)));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(distanceFromCheckpoint, rocksL1.getDistanceFromCheckpoint());
  }

  @Test
  void incrementalKeyListCheckpointIdSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.CHECKPOINT_ID)
        .build(), ID_2.toEntity()));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(ID_2, rocksL1.getCheckpointId());
  }

  @Test
  void incrementKeyListDistanceFromCheckpointSetEqualsExistingCompleteList() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final Long distanceFromCheckpoint = 32L;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.DISTANCE_FROM_CHECKPOINT)
        .build(), Entity.ofNumber(distanceFromCheckpoint)));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(distanceFromCheckpoint, rocksL1.getDistanceFromCheckpoint());
  }

  @Test
  void incrementKeyListCheckpointIdSetEqualsExistingCompleteList() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.CHECKPOINT_ID).build(), ID_2.toEntity()));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(ID_2, rocksL1.getCheckpointId());
  }

  @Test
  void completeKeyListRemoveFirst() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST)
        .position(0).build()));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(Collections.singletonList(Id.build("id2")), rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void completeKeyListRemoveLast() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST)
        .position(1).build()));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(Collections.singletonList(Id.build("id1")), rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void completeKeyListSetEqualsFirst() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).position(0).build(),
        Id.build("id3").toEntity()));
    rocksL1.update(updateExpression);

    final List<Id> expectedList = Lists.newArrayList(Id.build("id3"), Id.build("id2"));
    Assertions.assertEquals(expectedList, rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void completeKeyListSetEqualsAllWithList() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).build(),
        Entity.ofList(Id.build("id3").toEntity(), Id.build("id4").toEntity())));
    rocksL1.update(updateExpression);

    final List<Id> expectedList = Lists.newArrayList(Id.build("id3"), Id.build("id4"));
    Assertions.assertEquals(expectedList, rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void completeKeyListSetEqualsAllWithId() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).build(),
        Id.build("id3").toEntity()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void completeKeyListSetAppendToWithId() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).build(),
        Id.build("id3").toEntity()));
    rocksL1.update(updateExpression);

    final List<Id> expectedList = Lists.newArrayList(Id.build("id1"), Id.build("id2"), Id.build("id3"));
    Assertions.assertEquals(expectedList, rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void completeKeyListSetAppendToWithList() {
    rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).build(),
        Entity.ofList(Id.build("id3").toEntity(), Id.build("id4").toEntity())));
    rocksL1.update(updateExpression);

    final List<Id> expectedList = Lists.newArrayList(Id.build("id1"), Id.build("id2"), Id.build("id3"), Id.build("id4"));
    Assertions.assertEquals(expectedList, rocksL1.getCompleteKeyList().collect(Collectors.toList()));
  }

  @Test
  void ancestorsRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
            RemoveClause.of(ExpressionPath.builder(RocksL1.ANCESTORS).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void ancestorsRemoveSingle() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.ANCESTORS).position(0).build()));
    final List<Id> expectedList = rocksL1.getAncestors().skip(1).collect(Collectors.toList());

    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getAncestors().collect(Collectors.toList());
    Assertions.assertEquals(expectedList, updatedList);
  }

  @Test
  void ancestorsSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.ANCESTORS).build(), ID_ENTITY_LIST));
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getAncestors().collect(Collectors.toList());
    Assertions.assertEquals(ID_LIST, updatedList);
  }

  @Test
  void ancestorsSetAppendToListWithId() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.ANCESTORS).build(), ID_2.toEntity()));
    final List<Id> initialList = rocksL1.getAncestors().collect(Collectors.toList());
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getAncestors().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), 1);
    Assertions.assertEquals(ID_2, updatedList.get(updatedList.size() - 1));
  }

  @Test
  void ancestorsSetAppendToListWithList() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.ANCESTORS).build(), ID_ENTITY_LIST));
    final List<Id> initialList = rocksL1.getAncestors().collect(Collectors.toList());
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getAncestors().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), ID_LIST.size());
    Assertions.assertEquals(ID_LIST, updatedList.subList(initialList.size(), updatedList.size()));
  }

  @Test
  void childrenRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
        RemoveClause.of(ExpressionPath.builder(RocksL1.TREE).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void childrenSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.TREE).build(), ID_ENTITY_LIST));
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(ID_LIST, updatedList);
  }

  @Test
  void childrenSetAppendToListWithId() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.TREE).build(), ID_2.toEntity()));
    final List<Id> initialList = rocksL1.getChildren().collect(Collectors.toList());
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), 1);
    Assertions.assertEquals(ID_2, updatedList.get(updatedList.size() - 1));
  }

  @Test
  void childrenSetAppendToListWithList() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.TREE).build(), ID_ENTITY_LIST));
    final List<Id> initialList = rocksL1.getChildren().collect(Collectors.toList());
    rocksL1.update(updateExpression);
    final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), ID_LIST.size());
    Assertions.assertEquals(ID_LIST, updatedList.subList(initialList.size(), updatedList.size()));
  }

  @Test
  void keyMutationsRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
            RemoveClause.of(ExpressionPath.builder(RocksL1.KEY_MUTATIONS).build()));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void keyMutationsRemoveSingle() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.KEY_MUTATIONS).position(0).build()));

    final List<Integer> expectedKeyMutationTypes = new ArrayList<>();
    for (int i = 1; i < rocksL1.getKeyMutationsCount(); i++) {
      expectedKeyMutationTypes.add(rocksL1.getKeyMutationType(i));
    }
    final List<List<String>> expectedKeyMutationKeys = new ArrayList<>();
    for (int i = 1; i < rocksL1.getKeyMutationsCount(); i++) {
      expectedKeyMutationKeys.add(rocksL1.getKeyMutationKeys(i));
    }

    rocksL1.update(updateExpression);
    Assertions.assertEquals(expectedKeyMutationTypes.size(), rocksL1.getKeyMutationsCount());
    for (int i = 0; i < expectedKeyMutationTypes.size(); i++) {
      Assertions.assertEquals(expectedKeyMutationTypes.get(i), rocksL1.getKeyMutationType(i));
      Assertions.assertEquals(expectedKeyMutationKeys.get(i), rocksL1.getKeyMutationKeys(i));
    }
  }

  @Test
  void keyMutationsSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.KEY_MUTATIONS).build(), ID_ENTITY_LIST));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void keyMutationsSetEqualsSingle() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.KEY_MUTATIONS).position(0).build(), ID_ENTITY_LIST));
    updateTestFails(rocksL1, updateExpression);
  }

  @Test
  void keyMutationsSetEqualsType() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath
            .builder(RocksL1.KEY_MUTATIONS)
            .position(0)
            .name(RocksL1.KEY_MUTATIONS_MUTATION_TYPE)
            .build(), Entity.ofNumber(ValueProtos.KeyMutation.MutationType.REMOVAL_VALUE)));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(ValueProtos.KeyMutation.MutationType.REMOVAL_VALUE, rocksL1.getKeyMutationType(0));
  }

  @Test
  void keyMutationsSetEqualsKey() {
    final int index = 0;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath
            .builder(RocksL1.KEY_MUTATIONS)
            .position(index)
            .name(RocksL1.KEY_MUTATIONS_KEY)
            .build(), Entity.ofList(Entity.ofString("foo"), Entity.ofString("bar"))));
    rocksL1.update(updateExpression);
    Assertions.assertEquals(Lists.newArrayList("foo", "bar"), rocksL1.getKeyMutationKeys(index));
  }

  @Test
  void keyMutationsSetEqualsKeySingle() {
    final int keyMutationsIndex = 0;
    final int keyMutationsKeyIndex = 0;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath
            .builder(RocksL1.KEY_MUTATIONS)
            .position(keyMutationsIndex)
            .name(RocksL1.KEY_MUTATIONS_KEY)
            .position(keyMutationsKeyIndex)
            .build(), Entity.ofString("foo")));

    final List<String> expectedList = new ArrayList<>(rocksL1.getKeyMutationKeys(keyMutationsIndex));
    expectedList.set(keyMutationsKeyIndex, "foo");

    rocksL1.update(updateExpression);
    Assertions.assertEquals(expectedList, rocksL1.getKeyMutationKeys(keyMutationsIndex));
  }

  @Test
  void keyMutationsSetAppendToListSingle() {
    final int index = 0;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath
            .builder(RocksL1.KEY_MUTATIONS)
            .position(index)
            .name(RocksL1.KEY_MUTATIONS_KEY)
            .build(), Entity.ofString("foo")));

    final List<String> expectedList = new ArrayList<>(rocksL1.getKeyMutationKeys(index));
    expectedList.add("foo");

    rocksL1.update(updateExpression);
    Assertions.assertEquals(expectedList, rocksL1.getKeyMutationKeys(index));
  }

  @Test
  void keyMutationsSetAppendToListWithList() {
    final int index = 0;
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath
            .builder(RocksL1.KEY_MUTATIONS)
            .position(index)
            .name(RocksL1.KEY_MUTATIONS_KEY)
            .build(), Entity.ofList(Entity.ofString("foo"), Entity.ofString("bar"))));

    final List<String> expectedList = new ArrayList<>(rocksL1.getKeyMutationKeys(index));
    expectedList.add("foo");
    expectedList.add("bar");

    rocksL1.update(updateExpression);
    Assertions.assertEquals(expectedList, rocksL1.getKeyMutationKeys(index));
  }
}
