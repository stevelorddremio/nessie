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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.Random;

import org.bson.BsonDocument;
import org.bson.internal.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.collect.ImmutableMap;

class TestMongoDBExpressions {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity ONE = Entity.ofString("one");
  private static final Entity TWO = Entity.ofString("two");
  private static final Entity THREE = Entity.ofString("three");

  private static final Entity AV0 = Entity.ofBoolean(true);
  private static final Entity AV1 = Entity.ofBoolean(false);
  private static final Entity AV2 = SampleEntities.createStringEntity(RANDOM, 10);
  private static final Entity AV3 = Entity.ofList(ONE, TWO, THREE);
  private static final Entity AV4 =
    Entity.ofMap(ImmutableMap.of(
      "key_one", ONE,
      "key_two", TWO,
      "key_three", THREE));
  private static final Entity AV5 = Entity.ofNumber(65535);
  private static final byte[] BYTES_8 = SampleEntities.createBinary(RANDOM, 8);
  private static final Entity AV6 = Entity.ofBinary(BYTES_8);
  private static final byte[] BYTES_20 = SampleEntities.createBinary(RANDOM, 20);
  private static final Entity AV7 = Entity.ofBinary(BYTES_20);

  private static final Entity ID = Entity.ofBinary(SampleEntities.createId(RANDOM).toBytes());

  private static final ExpressionPath P0 = ExpressionPath.builder("p0").build();
  private static final ExpressionPath P1 = ExpressionPath.builder("p1").build();
  private static final ExpressionPath P2 = ExpressionPath.builder("p2").position(2).build();
  private static final ExpressionPath P3 = ExpressionPath.builder("p3").name("p4").build();
  private static final ExpressionPath COMMIT_0_ID = ExpressionPath.builder("commits").position(0).name("id").build();

  private static final ExpressionPath KEY_NAME = ExpressionPath.builder(Store.KEY_NAME).build();

  private static final BsonConditionExpressionVisitor BSON_CONDITION_EXPRESSION_VISITOR = new BsonConditionExpressionVisitor();
  private static final BsonValueVisitor BSON_EXPRESSION_FUNCTION_VISITOR = new BsonValueVisitor();

  // Single ExpressionFunction equals tests
  @Test
  void conditionExpressionEqualsBooleanTrue() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0));
    equals("{\"$and\": [{\"p0\": true}]}", ex);
  }

  @Test
  void conditionExpressionEqualsBooleanFalse() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV1));
    equals("{\"$and\": [{\"p0\": false}]}", ex);
  }

  @Test
  void conditionExpressionEqualsList() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV3));
    equals("{\"$and\": [{\"p0\": [\"one\", \"two\", \"three\"]}]}", ex);
  }

  @Test
  void conditionExpressionEqualsMap() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV4));
    equals("{\"$and\": [{\"p0\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", ex);
  }

  @Test
  void conditionExpressionEqualsNumber() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV5));
    equals("{\"$and\": [{\"p0\": 65535}]}", ex);
  }

  @Test
  void conditionExpressionEqualsString() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, THREE));
    equals("{\"$and\": [{\"p0\": \"three\"}]}", ex);
  }

  @Test
  void conditionExpressionEqualsBinary8() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV6));
    equals(String.format("{\"$and\": [{\"p0\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_8)), ex);
  }

  @Test
  void conditionExpressionEqualsBinary20() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV7));
    equals(String.format("{\"$and\": [{\"p0\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_20)), ex);
  }

  // Single ExpressionFunction array equals tests
  @Test
  void conditionExpressionArrayEqualsBooleanTrue() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV0));
    equals("{\"$and\": [{\"p2.2\": true}]}", ex);
  }

  @Test
  void conditionExpressionArrayEqualsBooleanFalse() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV1));
    equals("{\"$and\": [{\"p2.2\": false}]}", ex);
  }

  @Test
  void conditionExpressionArrayEqualsList() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV3));
    equals("{\"$and\": [{\"p2.2\": [\"one\", \"two\", \"three\"]}]}", ex);
  }

  @Test
  void conditionExpressionArrayEqualsMap() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV4));
    equals("{\"$and\": [{\"p2.2\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", ex);
  }

  @Test
  void conditionExpressionArrayEqualsNumber() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV5));
    equals("{\"$and\": [{\"p2.2\": 65535}]}", ex);
  }

  @Test
  void conditionExpressionArrayEqualsString() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, THREE));
    equals("{\"$and\": [{\"p2.2\": \"three\"}]}", ex);
  }

  @Test
  void conditionExpressionArrayBinary8() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV6));
    equals(String.format("{\"$and\": [{\"p2.2\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_8)), ex);
  }

  @Test
  void conditionExpressionArrayEqualsBinary20() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P2, AV7));
    equals(String.format("{\"$and\": [{\"p2.2\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_20)), ex);
  }

  // Single ExpressionFunction array equals tests
  @Test
  void conditionExpressionSubpathEqualsBooleanTrue() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV0));
    equals("{\"$and\": [{\"p3.p4\": true}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsBooleanFalse() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV1));
    equals("{\"$and\": [{\"p3.p4\": false}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsList() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV3));
    equals("{\"$and\": [{\"p3.p4\": [\"one\", \"two\", \"three\"]}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsMap() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV4));
    equals("{\"$and\": [{\"p3.p4\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsNumber() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV5));
    equals("{\"$and\": [{\"p3.p4\": 65535}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsString() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, THREE));
    equals("{\"$and\": [{\"p3.p4\": \"three\"}]}", ex);
  }

  @Test
  void conditionExpressionSubpathEqualsBinary8() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV6));
    equals(String.format("{\"$and\": [{\"p3.p4\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_8)), ex);
  }

  @Test
  void conditionExpressionSubpathEqualsBinary20() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P3, AV7));
    equals(String.format("{\"$and\": [{\"p3.p4\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}]}", Base64.encode(BYTES_20)), ex);
  }

  // Single ExpressionFunction size tests
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

  // Multiple ExpressionFunctions
  @Test
  void conditionExpressionTwoEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0), ExpressionFunction.equals(P1, AV1));
    equals("{\"$and\": [{\"p0\": true}, {\"p1\": false}]}", ex);
  }

  @Test
  void conditionExpressionThreeEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0),
      ExpressionFunction.equals(P1, AV1), ExpressionFunction.equals(P2, AV2));
    equals(String.format("{\"$and\": [{\"p0\": true}, {\"p1\": false}, {\"p2.2\": \"%s\"}]}", AV2.getString()), ex);
  }

  @Test
  void conditionExpressionFourEquals() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(P0, AV0),
      ExpressionFunction.equals(P1, AV1), ExpressionFunction.equals(P2, AV2), ExpressionFunction.equals(P3, AV3));
    equals(String.format("{\"$and\": [{\"p0\": true}, {\"p1\": false}, {\"p2.2\": \"%s\"}, {\"p3.p4\": [\"one\", \"two\", \"three\"]}]}", AV2.getString()), ex);
  }

  // Negative tests
  @Test
  void conditionIfNotExistsNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.ifNotExists(P0, AV0));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionSizeNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(P0));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionListAppendNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.appendToList(P0, AV0));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionAttributeNotExistsNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.attributeNotExists(P0));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionNestedIfNotExistsNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.ifNotExists(P0, AV0), TWO));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionNestedAppendToListNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.appendToList(P0, AV0), TWO));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionNestedAttributeNotExistsNotSupported() {
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.attributeNotExists(P0), TWO));
    failsUnsupportedOperationException(ex);
  }

  // other tests
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

  private void failsUnsupportedOperationException(ConditionExpression input) {
    assertThrows(UnsupportedOperationException.class, () -> input.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }
}
