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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
import com.mongodb.client.model.Filters;

public class TestMongoDbExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);
  private final Entity av2 = Entity.ofString("mystr");

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();

  @Disabled
  @Test
  void conditionExpressionEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).acceptAlias(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    //TODO change to use Filters
    assertEquals(new Document(p0.asString(), av0.getBoolean()), ex.accept(conditionExpressionVisitor));
  }

  @Disabled
  @Test
  void conditionExpressionArrayEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p2, av2)).acceptAlias(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    //TODO change to use Filters
    assertEquals(new Document(p2.asString(), av2.getString()), ex.accept(conditionExpressionVisitor));
  }

  @Disabled
  @Test
  void conditionExpressionSize() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0))
      .acceptAlias(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    //TODO change to use Filters
    assertEquals(new Document("$size", p0.asString()), ex.accept(conditionExpressionVisitor));
  }

  // TODO: Check expected Document is correct.
  // TODO: fix getting Value correctly in alias.
  @Test
  void conditionExpressionAndEquals() {
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    ConditionExpressionAliasVisitor conditionExpressionAliasVisitor = new MongoDBConditionExpressionAliasVisitor();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1))
      .acceptAlias(conditionExpressionAliasVisitor, c);
    BsonConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    Bson expected = Filters.and(
      Filters.eq(p0.asString(), av0.getBoolean()),
      Filters.eq(p1.asString(), av1.getBoolean()));
    assertEquals(expected, ex.accept(conditionExpressionVisitor));
  }

  @Disabled
  @Test
  void equals() {
    ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    MongoDBAliasCollectorImpl c = new MongoDBAliasCollectorImpl();
    MongoDBExpressionFunctionAliasVisitor expressionFunctionAliasVisitor = new MongoDBExpressionFunctionAliasVisitor();
    ExpressionFunction f2 = f.acceptAlias(expressionFunctionAliasVisitor, c);
    BsonExpressionFunctionVisitor expressionFunctionVisitor = new BsonExpressionFunctionVisitor();
    //TODO change to use Filters
    assertEquals(new Document("foo", av0.getBoolean()), f2.accept(expressionFunctionVisitor));
  }

}
