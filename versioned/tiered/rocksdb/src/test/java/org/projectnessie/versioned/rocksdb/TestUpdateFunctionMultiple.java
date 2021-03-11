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
    protected void remove(ExpressionPath path) {
      if (path.accept(PathPattern.exact(LIST_VALUE).anyPosition())) {
        listValue.remove(path.getRoot().getChild().get().asPosition().getPosition());
      } else if (path.accept(PathPattern.exact(NESTED_VALUE).anyPosition())) {
        nestedValue.remove(path.getRoot().getChild().get().asPosition().getPosition());
      } else if (path.accept(PathPattern.exact(NESTED_VALUE).anyPosition().anyPosition())) {
        int innerIndex = path.getRoot().getChild().get().getChild().get().asPosition().getPosition();
        nestedValue.get(path.getRoot().getChild().get().asPosition().getPosition()).remove(innerIndex);
      }
    }

    @Override
    protected boolean fieldIsList(ExpressionPath path) {
      return path.accept(PathPattern.exact(LIST_VALUE))
          || path.accept(PathPattern.exact(NESTED_VALUE))
          || path.accept(PathPattern.exact(NESTED_VALUE).anyPosition());
    }

    @Override
    protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
      if (path.accept(PathPattern.exact(LIST_VALUE))) {
        listValue.addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
      } else if (path.accept(PathPattern.exact(NESTED_VALUE))) {
        nestedValue.addAll(valuesToAdd.stream().map(v ->
            v.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
        ).collect(Collectors.toList()));
      } else if (path.accept(PathPattern.exact(NESTED_VALUE).anyPosition())) {
        nestedValue.get(getPathSegmentAsPosition(path, 1))
            .addAll(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
      }
    }

    @Override
    protected void set(ExpressionPath path, Entity newValue) {
      if (path.accept(PathPattern.exact(SCALAR_VALUE))) {
        scalarValue = newValue.getString();
      } else if (path.accept(PathPattern.exact(LIST_VALUE))) {
        listValue = newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList());
      } else if (path.accept(PathPattern.exact(LIST_VALUE).anyPosition())) {
        listValue.set(getPathSegmentAsPosition(path, 1), newValue.getBinary());
      } else if (path.accept(PathPattern.exact(NESTED_VALUE))) {
        nestedValue = newValue.getList().stream().map(v ->
            v.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
        ).collect(Collectors.toList());
      } else if (path.accept(PathPattern.exact(NESTED_VALUE).anyPosition())) {
        nestedValue.set(
            getPathSegmentAsPosition(path, 1),
            newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList())
        );
      } else if (path.accept(PathPattern.exact(NESTED_VALUE).anyPosition().anyPosition())) {
        nestedValue.get(getPathSegmentAsPosition(path, 1)).set(
            getPathSegmentAsPosition(path, 2),
            newValue.getBinary()
        );
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
