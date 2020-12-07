package com.dremio.nessie.versioned.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.condition.BsonConditionExpression;
import com.dremio.nessie.versioned.impl.condition.BsonExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
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
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertTrue(new Document("p0", ":v0").equals(bsonConditionExpression.to(ex)));
  }

  @Test
  void conditionExpressionSize() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    //TODO check alias if it needs updating.
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(p0)).alias(c);
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertTrue(new Document("$size", "p0").equals(bsonConditionExpression.to(ex)));
  }

  // TODO: Add ADD handling
  @Disabled
  @Test
  void conditionExpressionAndEquals() {
    AliasCollectorImpl c = new AliasCollectorImpl();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(p0, av0), ExpressionFunction.equals(p1, av1)).alias(c);
    BsonConditionExpression bsonConditionExpression = new BsonConditionExpression();
    assertEquals(new Document("$and", "[{p0, :v0}, {p1, :v1}]"),bsonConditionExpression.to(ex));
  }

  @Test
  void equals() {
    ExpressionFunction f = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), av0);
    AliasCollectorImpl c = new AliasCollectorImpl();
    ExpressionFunction f2 = f.alias(c);
    BsonExpressionFunction bsonExpressionFunction = new BsonExpressionFunction();
    assertEquals(new Document("foo", ":v0"), bsonExpressionFunction.as(f2));
  }

}
