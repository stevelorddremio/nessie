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
package com.dremio.nessie.versioned.impl.condition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;


import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Store;
import com.mongodb.client.model.Filters;

public class TestMongoDbExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);
  private final Entity av2 = Entity.ofString("mystr");
  private Random random = new Random(8612341233543L);

  private final Entity id  = Entity.ofString(((HasId) SampleEntities.createL1(random)).getId().toString());

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();
  private final String two = "2";
  private final ExpressionPath pathTwo = ExpressionPath.builder(two).build();
  private final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();

  @Test
  void conditionExpressionEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).accept(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor bsonConditionExpressionVisitor = new BsonConditionExpressionVisitor();
    assertEquals(new Document(p0.asString(), av0.getBoolean()).toString(), ex.accept(bsonConditionExpressionVisitor).toString());
  }

  @Test
  void conditionExpressionArrayEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p2, av2)).accept(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor bsonConditionExpressionVisitor = new BsonConditionExpressionVisitor();
    Bson expected = Filters.eq(p2.asString(), av2.getString());
    assertTrue(new Document(p2.asString(), av2.getString()).equals(ex.accept(bsonConditionExpressionVisitor)));
  }

  @Test
  void conditionExpressionSize() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0))
        .accept(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    assertEquals(new Document("$size", p0.asString()), ex.accept(conditionExpressionVisitor));
  }

  @Test
  void conditionExpressionOfBranchAndWithSize() {
    MongoDBAliasCollectorImpl collector = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();

    ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(
        ExpressionPath.builder("commits").position(0).name("id").build(), id));
    conditionExpression = conditionExpression.and(
      ExpressionFunction.size(pathTwo))
      .accept(conditionExpressionAliasVisitor, collector);

    BsonConditionExpressionVisitor bsonConditionExpressionVisitor = new BsonConditionExpressionVisitor();
    Document expected = new Document("$and", Arrays.asList(
        new Document("commits[0].id", id.getString()),
        new Document("$size", two)));
    assertEquals(expected.toString(), conditionExpression.accept(bsonConditionExpressionVisitor).toString());
  }

  @Test
  void conditionExpressionAndEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1))
        .accept(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    Document expected = new Document("$and", Arrays.asList(
        new Document(p0.asString(), av0.getBoolean()),
        new Document(p1.asString(), av1.getBoolean())));
    assertEquals(expected.toString(), ex.accept(conditionExpressionVisitor).toString());
  }

  @Test
  void equals() {
    ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    MongoDBExpressionFunctionAliasVisitor expressionFunctionAliasVisitor = new MongoDBExpressionFunctionAliasVisitor();
    ExpressionFunction f2 = f.accept(expressionFunctionAliasVisitor, c);
    BsonExpressionFunctionVisitor bsonExpressionFunctionVisitor = new BsonExpressionFunctionVisitor();
    assertEquals(new Document("foo", av0.getBoolean()).toString(), f2.accept(bsonExpressionFunctionVisitor).toString());
  }

  @Test
  void conditionExpressionKeyHadId() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(keyName, id)).accept(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor bsonConditionExpressionVisitor = new BsonConditionExpressionVisitor();
    assertEquals(new Document(keyName.asString(), id.getString()).toString(), ex.accept(bsonConditionExpressionVisitor).toString());
  }
}
