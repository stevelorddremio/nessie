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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.BsonConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.BsonExpressionFunctionVisitor;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.MongoDBAliasCollectorImpl;
import com.dremio.nessie.versioned.impl.condition.MongoDBConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.impl.condition.MongoDBExpressionFunctionAliasVisitor;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.model.Filters;

class TestMongoDbExpressions {
  private static final Entity av0 = Entity.ofBoolean(true);
  private static final Entity av1 = Entity.ofBoolean(false);
  private static final Entity av2 = Entity.ofString("mystr");
  private static final Random random = new Random(8612341233543L);

  private static final Entity id  = Entity.ofString(((HasId) SampleEntities.createL1(random)).getId().toString());

  private static final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private static final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private static final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();
  private static final ExpressionPath commit0Id = ExpressionPath.builder("commits").position(0).name("id").build();

  private static final String two = "2";
  private static final ExpressionPath pathTwo = ExpressionPath.builder(two).build();
  private static final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();

  private static final MongoDBAliasCollectorImpl collector = new MongoDBAliasCollectorImpl();
  private static final ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
  private static final MongoDBExpressionFunctionAliasVisitor expressionFunctionAliasVisitor = new MongoDBExpressionFunctionAliasVisitor();
  private static final BsonConditionExpressionVisitor bsonConditionExpressionVisitor = new BsonConditionExpressionVisitor();
  private static final BsonExpressionFunctionVisitor bsonExpressionFunctionVisitor = new BsonExpressionFunctionVisitor();

  @Test
  void conditionExpressionEquals() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).accept(conditionExpressionAliasVisitor, collector);
    equals(new Document(p0.asString(), av0.getBoolean()), ex.accept(bsonConditionExpressionVisitor));
  }

  @Test
  void conditionExpressionArrayEquals() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p2, av2)).accept(conditionExpressionAliasVisitor, collector);
    equals(new Document(p2.asString(), av2.getString()), ex.accept(bsonConditionExpressionVisitor));
  }

  @Test
  void conditionExpressionSize() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0)).accept(conditionExpressionAliasVisitor, collector);
    equals(new Document("$size", p0.asString()), ex.accept(bsonConditionExpressionVisitor));
  }

  @Test
  void conditionExpressionOfBranchAndWithSize() {
    ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(commit0Id, id));
    conditionExpression = conditionExpression.and(ExpressionFunction.size(pathTwo))
      .accept(conditionExpressionAliasVisitor, collector);

    final Bson expected = Filters.and(ImmutableList.of(
        new Document(commit0Id.asString(), id.getString()),
        new Document("$size", two)));
    equals(expected, conditionExpression.accept(bsonConditionExpressionVisitor));
  }

  @Test
  void conditionExpressionAndEquals() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1))
        .accept(conditionExpressionAliasVisitor, collector);
    final Bson expected = Filters.and(
        new Document(p0.asString(), av0.getBoolean()),
        new Document(p1.asString(), av1.getBoolean()));
    equals(expected, ex.accept(bsonConditionExpressionVisitor));
  }

  @Test
  void equalsExpression() {
    ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    ExpressionFunction aliasedExpressionFunction = expressionFunction.accept(expressionFunctionAliasVisitor, collector);
    equals(new Document("foo", av0.getBoolean()), aliasedExpressionFunction.accept(bsonExpressionFunctionVisitor));
  }

  @Test
  void conditionExpressionKeyHadId() {
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(keyName, id)).accept(conditionExpressionAliasVisitor, collector);
    equals(new Document(keyName.asString(), id.getString()), ex.accept(bsonConditionExpressionVisitor));
  }

  private void equals(Bson expected, Bson actual) {
    assertEquals(expected.toString(), actual.toString());
  }
}
