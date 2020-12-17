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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;

import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.BsonConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.BsonExpressionFunctionVisitor;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.MongoDBConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.impl.condition.MongoDBExpressionFunctionAliasVisitor;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;
import com.mongodb.client.model.Filters;

class TestMongoDbExpressions {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity AV0 = Entity.ofBoolean(true);
  private static final Entity AV1 = Entity.ofBoolean(false);
  private static final Entity AV2 = SampleEntities.createStringEntity(RANDOM, 10);

  private static final Entity ID = Entity.ofString((SampleEntities.createL1(RANDOM)).getId().toString());

  private static final ExpressionPath P0 = ExpressionPath.builder("p0").build();
  private static final ExpressionPath P1 = ExpressionPath.builder("p1").build();
  private static final ExpressionPath P2 = ExpressionPath.builder("p2").position(2).build();
  private static final ExpressionPath COMMIT_0_ID = ExpressionPath.builder("commits").position(0).name("id").build();

  private static final Entity ONE = Entity.ofNumber(1);
  private static final ExpressionPath KEY_NAME = ExpressionPath.builder(Store.KEY_NAME).build();

  private static final MongoDBAliasCollectorImpl COLLECTOR = new MongoDBAliasCollectorImpl();
  private static final ConditionExpressionAliasVisitor CONDITION_EXPRESSION_ALIAS_VISITOR = new MongoDBConditionExpressionAliasVisitor();
  private static final MongoDBExpressionFunctionAliasVisitor EXPRESSION_FUNCTION_ALIAS_VISITOR =
      new MongoDBExpressionFunctionAliasVisitor();
  private static final BsonConditionExpressionVisitor BSON_CONDITION_EXPRESSION_VISITOR = new BsonConditionExpressionVisitor();
  private static final BsonExpressionFunctionVisitor BSON_EXPRESSION_FUNCTION_VISITOR = new BsonExpressionFunctionVisitor();

  @Test
  void conditionExpressionEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0))
        .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    final Bson expected = Filters.and(Filters.eq(P0.asString(), AV0.getBoolean()));
    equals(expected, ex.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  @Test
  void conditionExpressionArrayEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV2))
        .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    final Bson expected = Filters.and(Filters.eq(P2.asString(), AV2.getString()));
    equals(expected, ex.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  @Test
  void conditionExpressionSize() {
    final ConditionExpression ex = ConditionExpression.of(
        ExpressionFunction.equals(
          ExpressionFunction.size(P0), ONE))
        .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);

    final Bson expected = Filters.and(Filters.size(P0.asString(), 1));
    equals(expected, ex.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  @Test
  void conditionExpressionOfBranchAndWithSize() {
    ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(COMMIT_0_ID, ID));
    conditionExpression = conditionExpression.and(ExpressionFunction.equals(ExpressionFunction.size(P0), ONE))
      .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);

    final Bson expected = Filters.and(
        Filters.eq(COMMIT_0_ID.asString(), ID.getString()),
        Filters.size(P0.asString(), 1));

    equals(expected, conditionExpression.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  @Test
  void conditionExpressionAndEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0), ExpressionFunction.equals(P1, AV1))
        .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    final Bson expected = Filters.and(
        Filters.eq(P0.asString(), AV0.getBoolean()),
        Filters.eq(P1.asString(), AV1.getBoolean()));
    equals(expected, ex.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  @Test
  void equalsExpression() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), AV0);
    final ExpressionFunction aliasedExpressionFunction = expressionFunction.accept(EXPRESSION_FUNCTION_ALIAS_VISITOR, COLLECTOR);
    final Bson expected = Filters.eq("foo", AV0.getBoolean());
    equals(expected, aliasedExpressionFunction.accept(BSON_EXPRESSION_FUNCTION_VISITOR));
  }

  @Test
  void conditionExpressionKeyHadId() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(KEY_NAME, ID))
        .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    final Bson expected = Filters.and(Filters.eq(KEY_NAME.asString(), ID.getString()));
    equals(expected, ex.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  private void equals(Bson expected, Bson actual) {
    assertEquals(expected.toString(), actual.toString());
  }
}
