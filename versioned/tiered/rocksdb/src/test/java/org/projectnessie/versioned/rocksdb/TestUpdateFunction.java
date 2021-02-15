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

import java.util.Random;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUpdateFunction {
  private static final Random RANDOM = new Random(getRandomSeed());

  private static final ExpressionPath L1_ID_PATH = ExpressionPath.builder(RocksL1.ID).build();
  private static final Id ID = SampleEntities.createId(RANDOM);
  private static final Id ID_2 = SampleEntities.createId(RANDOM);
  private static final Entity ID_ENTITY = ID.toEntity();

  protected static long getRandomSeed() {
    return -2938423452345L;
  }

  @Nested
  @DisplayName("RocksDBUpdateClauseVisitor tests")
  class RocksDBUpdateClauseVisitorTests {
    @Test
    void setEquals() {
      final SetClause setClause = SetClause.equals(L1_ID_PATH, ID_ENTITY);
      final UpdateFunction.SetFunction function =
          (UpdateFunction.SetFunction) setClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
      Assertions.assertEquals(UpdateFunction.Operator.SET, function.getOperator());
      Assertions.assertEquals(UpdateFunction.SetFunction.SubOperator.EQUALS, function.getSubOperator());
      Assertions.assertEquals(L1_ID_PATH, function.getPath());
      Assertions.assertEquals(ID_ENTITY, function.getValue());
    }

    @Test
    void setAppendToList() {
      final SetClause setClause = SetClause.appendToList(L1_ID_PATH, ID_ENTITY);
      final UpdateFunction.SetFunction function =
          (UpdateFunction.SetFunction) setClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
      Assertions.assertEquals(UpdateFunction.Operator.SET, function.getOperator());
      Assertions.assertEquals(UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST, function.getSubOperator());
      Assertions.assertEquals(L1_ID_PATH, function.getPath());
      Assertions.assertEquals(ID_ENTITY, function.getValue());
    }

    @Test
    void remove() {
      final RemoveClause removeClause = RemoveClause.of(L1_ID_PATH);
      final UpdateFunction.RemoveFunction function =
          (UpdateFunction.RemoveFunction) removeClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
      Assertions.assertEquals(UpdateFunction.Operator.REMOVE, function.getOperator());
      Assertions.assertEquals(L1_ID_PATH, function.getPath());
    }
  }

  @Nested
  @DisplayName("RocksL1 update() tests")
  class RocksL1Tests {
    @Test
    void removeId() {
      final RocksL1 rocksL1 = createL1(RANDOM);
      final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksL1.ID).build()));
      try {
        rocksL1.update(updateExpression);
        Assertions.fail("UnsupportedOperationException should have been thrown");
        //        Id id = rocksL1.getId();
        //        Assertions.assertNull(id);
      } catch (UnsupportedOperationException e) {
        // Expected result
      }
    }

    @Test
    void setEqualsId() {
      final RocksL1 rocksL1 = createL1(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.ID).build(), ID_2.toEntity()));
      rocksL1.update(updateExpression);
      Assertions.assertEquals(ID_2, rocksL1.getId());
    }

    @Test
    void setAppendToListId() {
      final RocksL1 rocksL1 = createL1(RANDOM);
      final UpdateExpression updateExpression =
          UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.ID).build(), ID_2.toEntity()));
      try {
        rocksL1.update(updateExpression);
        Assertions.fail("UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
        // Expected result
      }
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
