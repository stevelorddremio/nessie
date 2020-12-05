package com.dremio.nessie.versioned.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.condition.BsonConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.ImmutableUpdateExpression;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.dynamo.AliasCollectorImpl;
import com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil;

public class TestMongoDbExpressions {

  private final Entity av0 = Entity.ofBoolean(true);
  private final Entity av1 = Entity.ofBoolean(false);
  private final Entity av2 = Entity.ofString("mystr");

  private final ExpressionPath p0 = ExpressionPath.builder("p0").build();
  private final ExpressionPath p1 = ExpressionPath.builder("p1").build();
  private final ExpressionPath p2 = ExpressionPath.builder("p2").position(2).build();

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
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertTrue(new Document("p0", ":v0").equals(bsonConditionExpression.to(ex)));
  }

  @Test
  void conditionExpressionSizeBson() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO should not be using alias. Use unaliased value.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0)).alias(c);
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertTrue(new Document("$size", "p0").equals(bsonConditionExpression.to(ex)));
  }

  @Test
  void conditionExpressionComplexPathSizeBson() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO should not be using alias. Use unaliased value.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p2, av2)).alias(c);
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertEquals(new Document("p2", ":v2"), bsonConditionExpression.to(ex));
    assertTrue(new Document("p2", ":v2").equals(bsonConditionExpression.to(ex)));
  }
}
