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

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestUpdateFunctionBase {
  protected static final Random RANDOM = new Random(getRandomSeed());

  protected static final Id ID = SampleEntities.createId(RANDOM);
  protected static final Id ID_2 = SampleEntities.createId(RANDOM);
  private static final Id ID_3 = SampleEntities.createId(RANDOM);
  protected static final List<Id> ID_LIST = Arrays.asList(ID, ID_2, ID_3);
  protected static final Entity ID_ENTITY_LIST = Entity.ofList(ID.toEntity(), ID_2.toEntity(), ID_3.toEntity());

  protected static long getRandomSeed() {
    return -2938423452345L;
  }

  protected void idRemove(RocksBaseValue baseValue) {
    final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(baseValue.ID).build()));
    updateTestFails(baseValue, updateExpression);
  }

  protected void idSetEquals(RocksBaseValue baseValue) {
    final Id newId = SampleEntities.createId(RANDOM);
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(baseValue.ID).build(), newId.toEntity()));
    baseValue.update(updateExpression);
    Assertions.assertEquals(newId, baseValue.getId());
  }

  protected void idSetAppendToList(RocksBaseValue baseValue) {
    final Id newId = SampleEntities.createId(RANDOM);
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(baseValue.ID).build(), newId.toEntity()));
    updateTestFails(baseValue, updateExpression);
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
