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
import java.util.Arrays;
import java.util.Collections;
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
public class TestUpdateFunction {
  private static final Random RANDOM = new Random(getRandomSeed());

  private static final ExpressionPath L1_ID_PATH = ExpressionPath.builder(RocksL1.ID).build();
  private static final Id ID = SampleEntities.createId(RANDOM);
  private static final Id ID_2 = SampleEntities.createId(RANDOM);
  private static final Id ID_3 = SampleEntities.createId(RANDOM);
  private static final Entity ID_ENTITY = ID.toEntity();
  private static final List<Id> ID_LIST = Arrays.asList(ID, ID_2, ID_3);
  private static final Entity ID_ENTITY_LIST = Entity.ofList(ID.toEntity(), ID_2.toEntity(), ID_3.toEntity());

  protected static long getRandomSeed() {
    return -2938423452345L;
  }

  @Nested
  @DisplayName("RocksDBUpdateClauseVisitor tests")
  class RocksDBUpdateClauseVisitorTests {
    @Test
    void setEquals() {
      final SetClause setClause = SetClause.equals(L1_ID_PATH, ID_ENTITY);
      final UpdateFunction.SetFunction function = setTest(setClause);
      Assertions.assertEquals(UpdateFunction.SetFunction.SubOperator.EQUALS, function.getSubOperator());
    }

    @Test
    void setAppendToList() {
      final SetClause setClause = SetClause.appendToList(L1_ID_PATH, ID_ENTITY);
      final UpdateFunction.SetFunction function = setTest(setClause);
      Assertions.assertEquals(UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST, function.getSubOperator());
    }

    @Test
    void remove() {
      final RemoveClause removeClause = RemoveClause.of(L1_ID_PATH);
      final UpdateFunction.RemoveFunction function =
          (UpdateFunction.RemoveFunction) removeClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
      Assertions.assertEquals(UpdateFunction.Operator.REMOVE, function.getOperator());
      Assertions.assertEquals(L1_ID_PATH, function.getPath());
    }

    private UpdateFunction.SetFunction setTest(SetClause setClause) {
      final UpdateFunction.SetFunction function =
          (UpdateFunction.SetFunction) setClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
      Assertions.assertEquals(UpdateFunction.Operator.SET, function.getOperator());
      Assertions.assertEquals(L1_ID_PATH, function.getPath());
      Assertions.assertEquals(ID_ENTITY, function.getValue());
      return function;
    }
  }

  abstract class AbstractIdTests {
    void idRemove(RocksBaseValue baseValue) {
      final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(baseValue.ID).build()));
      updateTestFails(baseValue, updateExpression);
    }

    void idSetEquals(RocksBaseValue baseValue) {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(baseValue.ID).build(), newId.toEntity()));
      baseValue.update(updateExpression);
      Assertions.assertEquals(newId, baseValue.getId());
    }

    void idSetAppendToList(RocksBaseValue baseValue) {
      final Id newId = SampleEntities.createId(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(baseValue.ID).build(), newId.toEntity()));
      updateTestFails(baseValue, updateExpression);
    }
  }

  @Nested
  @DisplayName("RocksL1 update() tests")
  class RocksL1Tests extends AbstractIdTests {
    final RocksL1 rocksL1 = createL1(RANDOM);

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
            RemoveClause.of(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST).name(RocksL1.CHECKPOINT_ID).build()));
      updateTestFails(rocksL1, updateExpression);
    }

    @Test
    void incrementalKeyListRemove() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(
            RemoveClause.of(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST).name(RocksL1.DISTANCE_FROM_CHECKPOINT).build()));
      updateTestFails(rocksL1, updateExpression);
    }

    @Test
    void incrementalKeyListDistanceFromCheckpointSetEquals() {
      final Long distanceFromCheckpoint = 32L;
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST)
            .name(RocksL1.DISTANCE_FROM_CHECKPOINT).build(), Entity.ofNumber(distanceFromCheckpoint)));
      rocksL1.update(updateExpression);
      Assertions.assertEquals(distanceFromCheckpoint, rocksL1.getDistanceFromCheckpoint());
    }

    @Test
    void incrementalKeyListCheckpointIdSetEquals() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST)
          .name(RocksL1.CHECKPOINT_ID).build(), ID_2.toEntity()));
      rocksL1.update(updateExpression);
      Assertions.assertEquals(ID_2, rocksL1.getCheckpointId());
    }

    @Test
    void incrementKeyListDistanceFromCheckpointSetEqualsExistingCompleteList() {
      rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
      final Long distanceFromCheckpoint = 32L;
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST)
          .name(RocksL1.DISTANCE_FROM_CHECKPOINT).build(), Entity.ofNumber(distanceFromCheckpoint)));
      rocksL1.update(updateExpression);
      Assertions.assertEquals(distanceFromCheckpoint, rocksL1.getDistanceFromCheckpoint());
    }

    @Test
    void incrementKeyListCheckpointIdSetEqualsExistingCompleteList() {
      rocksL1.completeKeyList(Stream.of(Id.build("id1"), Id.build("id2")));
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST)
          .name(RocksL1.CHECKPOINT_ID).build(), ID_2.toEntity()));
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
    void childrenRemove() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(
            RemoveClause.of(ExpressionPath.builder(RocksL1.CHILDREN).build()));
      updateTestFails(rocksL1, updateExpression);
    }

    @Test
    void childrenSetEquals() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_ENTITY_LIST));
      rocksL1.update(updateExpression);
      final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
      Assertions.assertEquals(ID_LIST, updatedList);
    }

    @Test
    void childrenSetAppendToListWithId() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_2.toEntity()));
      final List<Id> initialList = rocksL1.getChildren().collect(Collectors.toList());
      rocksL1.update(updateExpression);
      final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
      Assertions.assertEquals(updatedList.size() - initialList.size(), 1);
      Assertions.assertEquals(ID_2, updatedList.get(updatedList.size() - 1));
    }

    @Test
    void childrenSetAppendToListWithList() {
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_ENTITY_LIST));
      final List<Id> initialList = rocksL1.getChildren().collect(Collectors.toList());
      rocksL1.update(updateExpression);
      final List<Id> updatedList = rocksL1.getChildren().collect(Collectors.toList());
      Assertions.assertEquals(updatedList.size() - initialList.size(), ID_LIST.size());
      Assertions.assertEquals(ID_LIST, updatedList.subList(initialList.size(), updatedList.size()));
    }
  }

  static void updateTestFails(RocksBaseValue rocksBaseValue, UpdateExpression updateExpression) {
    try {
      rocksBaseValue.update(updateExpression);
      Assertions.fail("UnsupportedOperationException should have been thrown");
    } catch (UnsupportedOperationException e) {
      // Expected result
    }
  }

  /**
   * Create a Sample L1 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  static RocksL1 createL1(Random random) {
    return (RocksL1) new RocksL1()
      .id(Id.EMPTY)
      .commitMetadataId(ID)
      .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
      .ancestors(Stream.generate(() -> ID).limit(RocksL1.SIZE))
      .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
      .incrementalKeyList(ID, 1);
  }

  /**
   * Create a String of random characters.
   * @param random random number generator to use.
   * @param numChars the size of the String.
   * @return the String of random characters.
   */
  static String createString(Random random, int numChars) {
    return random.ints('a', 'z' + 1)
      .limit(numChars)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
  }
}
