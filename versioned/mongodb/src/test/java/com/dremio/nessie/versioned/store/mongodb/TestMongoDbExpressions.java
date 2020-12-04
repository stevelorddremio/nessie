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

import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.condition.AddClause;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.ImmutableUpdateExpression;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.impl.condition.Value;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.dynamo.AliasCollectorImpl;
import com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil;

class TestMongoDbExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);
  private final Entity av2 = Entity.ofString("mystr");

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();

  @Disabled
  @Test
  void aliasNoop() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionPath aliased = ExpressionPath.builder("foo").build().alias(c);
    assertEquals("foo", aliased.asString());
    assertTrue(c.getAttributeNames().isEmpty());
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Disabled
  @Test
  void multiAlias() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionPath aliased = ExpressionPath.builder("a.b").name("c.d").position(4).name("e.f").build().alias(c);
    assertEquals("#f0.#f1[4].#f2", aliased.asString());
    assertEquals("a.b", c.getAttributeNames().get("#f0"));
    assertEquals("c.d", c.getAttributeNames().get("#f1"));
    assertEquals("e.f", c.getAttributeNames().get("#f2"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Disabled
  @Test
  void aliasReservedWord() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionPath aliased = ExpressionPath.builder("abort").build().alias(c);
    assertEquals("#abortXX", aliased.asString());
    assertEquals("abort", c.getAttributeNames().get("#abortXX"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Disabled
  @Test
  void aliasSpecialCharacter() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionPath aliased = ExpressionPath.builder("foo.bar").build().alias(c);
    assertEquals("#f0", aliased.asString());
    assertEquals("foo.bar", c.getAttributeNames().get("#f0"));
    assertTrue(c.getAttributesValues().isEmpty());
  }

  @Disabled
  @Test
  void aliasValue() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    Value v0 = Value.of(av0);
    Value v0p = v0.alias(c);
    assertEquals(":v0", v0p.getPath().asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
    assertTrue(c.getAttributeNames().isEmpty());
  }

  @Disabled
  @Test
  void equals() {
    ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionFunction f2 = f.alias(c);
    assertEquals("foo = :v0", f2.asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
  }

  @Disabled
  @Test
  void listAppend() {
    Value v0 = ExpressionFunction.appendToList(ExpressionPath.builder("p0").build(), Entity.ofList(av0));
    AliasCollectorImpl c = new AliasCollectorImpl();
    Value v0p = v0.alias(c);
    assertEquals("list_append(p0, :v0)", v0p.asString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0").l().get(0));
  }

  @Disabled
  @Test
  void updateAddClause() {
    UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        AddClause.addToSetOrNumber(p0, av2)
        ).build();
    AliasCollectorImpl c = new AliasCollectorImpl();
    UpdateExpression e0p = e0.alias(c);
    assertEquals(" ADD p0 :v0", e0p.toUpdateExpressionString());
    assertEquals(AttributeValueUtil.fromEntity(av2), c.getAttributesValues().get(":v0"));
  }

  @Disabled
  @Test
  void updateSetClause() {
    UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        SetClause.equals(p0, av0),
        SetClause.ifNotExists(p1, p0, av1),
        SetClause.appendToList(p1, Entity.ofList(av1))
        ).build();
    AliasCollectorImpl c = new AliasCollectorImpl();
    UpdateExpression e0p = e0.alias(c);
    assertEquals(" SET p0 = :v0, p1 = if_not_exists(p0, :v1), p1 = list_append(p1, :v2)", e0p.toUpdateExpressionString());
    assertEquals(AttributeValueUtil.fromEntity(av0), c.getAttributesValues().get(":v0"));
    assertEquals(AttributeValueUtil.fromEntity(av1), c.getAttributesValues().get(":v1"));
  }

  @Disabled
  @Test
  void updateRemoveClause() {
    UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        RemoveClause.of(p0),
        RemoveClause.of(p2)
        ).build();
    AliasCollectorImpl c = new AliasCollectorImpl();
    UpdateExpression e0p = e0.alias(c);
    assertEquals(" REMOVE p0, p2[2]", e0p.toUpdateExpressionString());
  }

  @Disabled
  @Test
  void updateMultiClause() {
    UpdateExpression e0 = ImmutableUpdateExpression.builder().addClauses(
        AddClause.addToSetOrNumber(p0, av2),
        SetClause.equals(p0, av0),
        SetClause.ifNotExists(p1, p0, av1),
        SetClause.appendToList(p1, av1),
        RemoveClause.of(p0),
        RemoveClause.of(p2)
        ).build();
    AliasCollectorImpl c = new AliasCollectorImpl();
    UpdateExpression e0p = e0.alias(c);
    assertEquals(" "
        + "ADD p0 :v0 "
        + "SET p0 = :v1, p1 = if_not_exists(p0, :v2), p1 = list_append(p1, :v3) "
        + "REMOVE p0, p2[2]", e0p.toUpdateExpressionString());
  }


  @Test
  void conditionExpression() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).alias(c);
    assertEquals("p0 = :v0", ex.toConditionExpressionString());
  }

  @Test
  void conditionExpressionEqualsBson() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO should not be using alias. Use unaliased value.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0)).alias(c);
    assertTrue(new Document("p0", ":v0").equals(ex.toConditionExpressionBson()));
  }

  @Test
  void conditionExpressionSizeBson() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO should not be using alias. Use unaliased value.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0)).alias(c);
    assertTrue(new Document("$size", "p0").equals(ex.toConditionExpressionBson()));
  }
}
