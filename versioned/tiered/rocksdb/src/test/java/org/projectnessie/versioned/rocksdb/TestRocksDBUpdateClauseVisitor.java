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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

@DisplayName("RocksDBUpdateClauseVisitor tests")
public class TestRocksDBUpdateClauseVisitor {
  private static final Random RANDOM = new Random(getRandomSeed());
  private static final Id ID = SampleEntities.createId(RANDOM);
  private static final Entity ID_ENTITY = ID.toEntity();
  private static final ExpressionPath L1_ID_PATH = ExpressionPath.builder(RocksL1.ID).build();

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

  protected static long getRandomSeed() {
    return -2938423452345L;
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
