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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.condition.BsonConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.BsonExpressionFunctionVisitor;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunctionVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.dynamo.AliasCollectorImpl;

public class TestMongoDbExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);
  private final Entity av2 = Entity.ofString("mystr");

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();

  @Test
  void conditionExpressionEquals() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO check alias if it needs updating.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).alias(c);
    ConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    assertTrue(new Document("p0", ":v0").equals(ex.accept(conditionExpressionVisitor)));
  }

  @Test
  void conditionExpressionSize() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO check alias if it needs updating.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0)).alias(c);
    ConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    assertTrue(new Document("$size", "p0").equals(ex.accept(conditionExpressionVisitor)));
  }

  // TODO: Check expected Document is correct.
  @Disabled
  @Test
  void conditionExpressionAndEquals() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1)).alias(c);
    ConditionExpressionVisitor conditionExpressionVisitor = new BsonConditionExpressionVisitor();
    // TODO can we use Filters here?
    assertEquals(new Document("$and", "[{p0, :v0}, {p1, :v1}]"), ex.accept(conditionExpressionVisitor));
    Document expected = new Document("$and", Arrays.asList(
      new Document(p0.asString(), av0.getBoolean()),
      new Document(p1.asString(), av1.getBoolean())));
    assertEquals(expected, ex.accept(conditionExpressionVisitor));
  }

  @Test
  void equals() {
    ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionFunction f2 = f.alias(c);
    ExpressionFunctionVisitor expressionFunctionVisitor = new BsonExpressionFunctionVisitor();
    assertEquals(new Document("foo", ":v0"), f2.accept(expressionFunctionVisitor));
  }

}
