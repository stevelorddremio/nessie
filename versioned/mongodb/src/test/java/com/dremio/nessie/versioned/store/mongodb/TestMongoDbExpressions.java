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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.Random;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.BsonConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.BsonValueVisitor;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;

class TestMongoDBExpressions {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity AV0 = Entity.ofBoolean(true);
  private static final Entity AV1 = Entity.ofBoolean(false);
  private static final Entity AV2 = SampleEntities.createStringEntity(RANDOM, 10);

  private static final Entity ID = Entity.ofBinary(SampleEntities.createId(RANDOM).toBytes());

  private static final ExpressionPath P0 = ExpressionPath.builder("p0").build();
  private static final ExpressionPath P1 = ExpressionPath.builder("p1").build();
  private static final ExpressionPath P2 = ExpressionPath.builder("p2").position(2).build();
  private static final ExpressionPath COMMIT_0_ID = ExpressionPath.builder("commits").position(0).name("id").build();

  private static final ExpressionPath KEY_NAME = ExpressionPath.builder(Store.KEY_NAME).build();

  private static final BsonConditionExpressionVisitor BSON_CONDITION_EXPRESSION_VISITOR = new BsonConditionExpressionVisitor();
  private static final BsonValueVisitor BSON_EXPRESSION_FUNCTION_VISITOR = new BsonValueVisitor();

  @Test
  void conditionExpressionEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0));
    equals("{\"$and\": [{\"p0\": true}]}", ex);
  }

  @Test
  void conditionExpressionArrayEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV2));
    equals(String.format("{\"$and\": [{\"p2.2\": \"%s\"}]}", AV2.getString()), ex);
  }

  @Test
  void conditionExpressionSize() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(P0), Entity.ofNumber(4)));

    equals("{\"$and\": [{\"p0\": {\"$size\": 4}}]}", ex);
  }

  @Test
  void conditionExpressionOfBranchAndWithSize() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(COMMIT_0_ID, ID));
    ex = ex.and(ExpressionFunction.equals(ExpressionFunction.size(P0), Entity.ofNumber(1)));

    equals(String.format("{\"$and\": [{\"commits.0.id\": %s}, {\"p0\": {\"$size\": 1}}]}", BsonValueVisitor.toMongoExpr(ID)), ex);
  }

  @Test
  void conditionExpressionAndEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0), ExpressionFunction.equals(P1, AV1));
    equals("{\"$and\": [{\"p0\": true}, {\"p1\": false}]}", ex);
  }

  @Test
  void equalsExpression() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), AV0);
    Assertions.assertEquals("{\"foo\": true}", expressionFunction.accept(BSON_EXPRESSION_FUNCTION_VISITOR));
  }

  @Test
  void conditionExpressionKeyHadId() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(KEY_NAME, ID));
    equals(String.format("{\"$and\": [{\"id\": %s}]}", BsonValueVisitor.toMongoExpr(ID)), ex);
  }

  private void equals(String expected, ConditionExpression input) {
    Assertions.assertEquals(BsonDocument.parse(expected), input.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }
}
