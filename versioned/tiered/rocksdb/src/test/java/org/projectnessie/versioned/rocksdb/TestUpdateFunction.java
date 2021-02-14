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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.store.Entity;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUpdateFunction {
  private static final Random RANDOM = new Random(8612341233543L);

  @Test
  void updateEquals() {
    final ExpressionPath expressionPath = ExpressionPath.builder(RocksL1.ID).build();
    final Entity id = SampleEntities.createId(RANDOM).toEntity();
    final SetClause setClause = SetClause.equals(expressionPath, id);
    final UpdateFunction function = setClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
    Assertions.assertEquals(UpdateFunction.Operator.SET, function.getOperator());
    Assertions.assertEquals(expressionPath, function.getPath());
    Assertions.assertEquals(id, function.getValue());
  }
}
