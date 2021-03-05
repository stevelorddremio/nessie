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

package org.projectnessie.versioned.rocksdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

import com.google.protobuf.ByteString;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksBase update() tests with multiple functions")
public class TestUpdateFunctionMultiple {
  private static final String SCALAR_VALUE = "scalarValue";
  private static final String LIST_VALUE = "listValue";
  private static final String NESTED_VALUE = "nestedValue";

  private static class RocksValueForTest extends RocksBaseValue<RocksValueForTest> {
    private String scalarValue;
    private List<ByteString> listValue;
    private List<List<ByteString>> nestedValue;

    public RocksValueForTest(String scalarValue, List<ByteString> listValue, List<List<ByteString>> nestedValue) {
      this.scalarValue = scalarValue;
      this.listValue = listValue;
      this.nestedValue = nestedValue;
    }

    @Override
    public void evaluate(Function function) throws ConditionFailedException {
    }

    @Override
    protected void remove(String fieldName, ExpressionPath.PathSegment path) {
      switch (fieldName) {
        case LIST_VALUE:
          listValue.remove(path.asPosition().getPosition());
          break;
        case NESTED_VALUE:
          if (path.getChild().isPresent()) {
            int innerIndex = path.getChild().get().asPosition().getPosition();
            nestedValue.get(path.asPosition().getPosition()).remove(innerIndex);
          } else {
            nestedValue.remove(path.asPosition().getPosition());
          }
          break;
        default:
          break;
      }
    }

    @Override
    protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
      if (LIST_VALUE.equals(fieldName) && (childPath == null || childPath.isPosition())) {
        return true;
      } else if (NESTED_VALUE.equals(fieldName)) {
        if (childPath == null || childPath.isPosition()
            || (childPath.isName() && childPath.getChild().isPresent() && childPath.getChild().get().isPosition())) {
          return true;
        }
      }

      return false;
    }

    @Override
    protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
      if (LIST_VALUE.equals(fieldName)) {
        listValue.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
      } else if (NESTED_VALUE.equals(fieldName)) {
        if (childPath != null) {
          nestedValue.get(childPath.asPosition().getPosition())
              .addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
        } else {
          nestedValue.addAll(valuesToAdd.stream().map(v ->
              v.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
          ).collect(Collectors.toList()));
        }
      }
    }

    @Override
    protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
      switch (fieldName) {
        case SCALAR_VALUE:
          scalarValue = newValue.getString();
          break;
        case LIST_VALUE:
          if (childPath == null) {
            listValue = newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList());
          } else {
            listValue.set(childPath.asPosition().getPosition(), newValue.getBinary());
          }
          break;
        case NESTED_VALUE:
          if (childPath == null) {
            nestedValue = newValue.getList().stream().map(v ->
              v.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
            ).collect(Collectors.toList());
          } else if (!childPath.getChild().isPresent()) {
            nestedValue.set(
                childPath.asPosition().getPosition(),
                newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
            );
          } else {
            nestedValue.get(childPath.asPosition().getPosition()).set(
                childPath.getChild().get().asPosition().getPosition(),
                newValue.getBinary()
            );
          }
          break;
        default:
          break;
      }
    }

    @Override
    byte[] build() {
      return new byte[0];
    }

    public String getScalarValue() {
      return scalarValue;
    }

    public List<ByteString> getListValue() {
      return listValue;
    }

    public List<List<ByteString>> getNestedValue() {
      return nestedValue;
    }
  }

  private RocksValueForTest testValue;

  @BeforeEach
  void createTestValue() {
    final String scalarValue = "foo";
    final List<ByteString> listValue = new ArrayList<>();
    final List<List<ByteString>> nestedValue = new ArrayList<>();
    final Random random = new Random();

    for (int i = 0; i < 20; i++) {
      byte[] value = new byte[20];
      random.nextBytes(value);
      listValue.add(ByteString.copyFrom(value));
    }

    for (int i = 0;i < 20; i++) {
      nestedValue.add(new ArrayList<>());

      for (int j = 0; j < 20; j++) {
        byte[] value = new byte[20];
        random.nextBytes(value);
        nestedValue.get(i).add(ByteString.copyFrom(value));
      }
    }

    testValue = new RocksValueForTest(scalarValue, listValue, nestedValue);
  }

  @Test
  void removeThenRemoveSameElement() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()))
        .and(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()));

    final List<ByteString> expectedList = testValue.getListValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void removeThenRemoveDifferentElement() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()))
        .and(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(1).build()));

    final List<ByteString> expectedList = testValue.getListValue().stream().skip(2).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void removeHigherLevelThenRemoveLowerLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()))
        .and(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build()));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void removeLowerLevelThenRemoveHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build()))
        .and(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void removeThenSetEqualsSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()))
        .and(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("foo").toEntity()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.remove(0);

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void removeThenSetEqualsLowerLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()))
        .and(SetClause.equals(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build(), Id.build("foo").toEntity()));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void removeThenSetEqualsHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build()))
        .and(SetClause.equals(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Entity.ofList(Id.build("foo").toEntity())));

    final List<List<ByteString>> expectedList = new ArrayList<>(testValue.getNestedValue());
    expectedList.set(0, Collections.singletonList(Id.build("foo").getValue()));

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void removeThenAppendSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()))
        .and(SetClause.appendToList(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Entity.ofList(Id.build("foo").toEntity())));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void removeThenAppendHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build()))
        .and(SetClause.appendToList(
          ExpressionPath.builder(NESTED_VALUE).build(),
          Entity.ofList(Entity.ofList(Id.build("foo").toEntity())))
        );

    final List<List<ByteString>> expectedList = new ArrayList<>(testValue.getNestedValue());
    final List<ByteString> innerList = new ArrayList<>(testValue.getNestedValue().get(0));
    innerList.remove(0);
    expectedList.set(0, innerList);
    expectedList.add(Collections.singletonList(Id.build("foo").getValue()));

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void setEqualsThenRemoveSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("foo").toEntity()))
        .and(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()));

    final List<ByteString> expectedList = testValue.getListValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void setEqualsThenRemoveLowerLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(LIST_VALUE).build(), Entity.ofList(Id.build("foo").toEntity())))
        .and(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()));

    testValue.update(updateExpression);
    Assertions.assertEquals(Collections.singletonList(Id.build("foo").getValue()), testValue.getListValue());
  }

  @Test
  void setEqualsThenRemoveHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(
          ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build(),
          Id.build("foo").toEntity())
        ).and(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void setEqualsThenSetEqualsSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("foo").toEntity()))
        .and(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("bar").toEntity()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.set(0, Id.build("bar").getValue());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void setEqualsThenSetEqualsLowerLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(
          ExpressionPath.builder(NESTED_VALUE).position(0).build(),
          Entity.ofList(Id.build("foo").toEntity()))
        ).and(SetClause.equals(ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build(), Id.build("bar").toEntity()));

    final List<List<ByteString>> expectedList = new ArrayList<>(testValue.getNestedValue());
    expectedList.set(0, Collections.singletonList(Id.build("foo").getValue()));

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void setEqualsThenSetEqualsHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(
          ExpressionPath.builder(NESTED_VALUE).position(0).position(0).build(),
          Id.build("foo").toEntity())
        ).and(SetClause.equals(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Entity.ofList(Id.build("bar").toEntity())));

    final List<List<ByteString>> expectedList = new ArrayList<>(testValue.getNestedValue());
    expectedList.set(0, Collections.singletonList(Id.build("bar").getValue()));

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void setEqualsThenAppendSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("foo").toEntity()))
        .and(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("bar").toEntity()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.set(0, Id.build("foo").getValue());
    expectedList.add(Id.build("bar").getValue());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void setEqualsThenAppendLowerLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(LIST_VALUE).build(), Entity.ofList(Id.build("foo").toEntity())))
        .and(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("bar").toEntity()));

    testValue.update(updateExpression);
    Assertions.assertEquals(Collections.singletonList(Id.build("foo").getValue()), testValue.getListValue());
  }

  @Test
  void appendThenRemoveSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("foo").toEntity()))
        .and(RemoveClause.of(ExpressionPath.builder(LIST_VALUE).position(0).build()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.add(Id.build("foo").getValue());
    expectedList.remove(0);

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void appendThenRemoveHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Id.build("foo").toEntity()))
        .and(RemoveClause.of(ExpressionPath.builder(NESTED_VALUE).position(0).build()));

    final List<List<ByteString>> expectedList = testValue.getNestedValue().stream().skip(1).collect(Collectors.toList());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void appendThenSetEqualsSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("foo").toEntity()))
        .and(SetClause.equals(ExpressionPath.builder(LIST_VALUE).position(0).build(), Id.build("bar").toEntity()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.set(0, Id.build("bar").getValue());
    expectedList.add(Id.build("foo").getValue());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }

  @Test
  void appendThenSetEqualsHigherLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Id.build("foo").toEntity()))
        .and(SetClause.equals(ExpressionPath.builder(NESTED_VALUE).position(0).build(), Entity.ofList(Id.build("bar").toEntity())));

    final List<List<ByteString>> expectedList = new ArrayList<>(testValue.getNestedValue());
    expectedList.set(0, Collections.singletonList(Id.build("bar").getValue()));

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getNestedValue());
  }

  @Test
  void appendThenAppendSameLevel() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("foo").toEntity()))
        .and(SetClause.appendToList(ExpressionPath.builder(LIST_VALUE).build(), Id.build("bar").toEntity()));

    final List<ByteString> expectedList = new ArrayList<>(testValue.getListValue());
    expectedList.add(Id.build("foo").getValue());
    expectedList.add(Id.build("bar").getValue());

    testValue.update(updateExpression);
    Assertions.assertEquals(expectedList, testValue.getListValue());
  }
}
