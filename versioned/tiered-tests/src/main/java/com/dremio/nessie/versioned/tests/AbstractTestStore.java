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
package com.dremio.nessie.versioned.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;

/**
 * Common class for testing public APIs of a Store.
 * This class should be moved to the versioned/tests project when it will not introduce a circular dependency.
 * @param <S> The type of the Store being tested.
 */
public abstract class AbstractTestStore<S extends Store> {
  private static final String MISSING = "Missing";

  protected static final Entity NON_EXISTENT = Entity.ofString(MISSING);


  protected static final ExpressionPath COMMITS = ExpressionPath.builder("commits").build();
  protected static final Entity ONE = Entity.ofNumber(1);
  protected static final Entity TWO = Entity.ofNumber(2);

  protected Random random;
  protected S store;

  /**
   * Create and start the store, if not already done.
   */
  @BeforeEach
  public void setup() {
    if (store == null) {
      this.store = createStore();
      this.store.start();
      random = new Random(getRandomSeed());
    }
  }

  /**
   * Reset the state of the store.
   */
  @AfterEach
  public void reset() {
    resetStoreState();
  }

  protected abstract S createStore();

  protected abstract long getRandomSeed();

  protected abstract void resetStoreState();

  @Test
  public void loadSingleL1() {
    putThenLoad(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  public void loadSingleL2() {
    putThenLoad(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  public void loadSingleL3() {
    putThenLoad(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  public void loadFragment() {
    putThenLoad(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  public void loadBranch() {
    putThenLoad(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  public void loadTag() {
    putThenLoad(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  public void loadCommitMetadata() {
    putThenLoad(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  public void loadValue() {
    putThenLoad(SampleEntities.createValue(random), ValueType.VALUE);
  }

  @Test
  public void putIfAbsentL1() {
    testPutIfAbsent(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  public void putIfAbsentL2() {
    testPutIfAbsent(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  public void putIfAbsentL3() {
    testPutIfAbsent(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  public void putIfAbsentFragment() {
    testPutIfAbsent(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  public void putIfAbsentBranch() {
    testPutIfAbsent(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  public void putIfAbsentTag() {
    testPutIfAbsent(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  public void putIfAbsentCommitMetadata() {
    testPutIfAbsent(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  public void putIfAbsentValue() {
    testPutIfAbsent(SampleEntities.createValue(random), ValueType.VALUE);
  }

  @Test
  public void save() {
    final L1 l1 = SampleEntities.createL1(random);
    final InternalRef branch = SampleEntities.createBranch(random);
    final InternalRef tag = SampleEntities.createTag(random);
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, l1),
        new SaveOp<>(ValueType.REF, branch),
        new SaveOp<>(ValueType.REF, tag)
    );
    store.save(saveOps);

    saveOps.forEach(s -> {
      try {
        final SimpleSchema<Object> schema = s.getType().getSchema();
        assertEquals(
            schema.itemToMap(s.getValue(), true),
            schema.itemToMap(store.loadSingle(s.getType(), s.getValue().getId()), true));
      } catch (NotFoundException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  public void putWithConditionValue() {
    putWithCondition(SampleEntities.createValue(random), ValueType.VALUE);
  }

  @Test
  public void putWithConditionBranch() {
    putWithCondition(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  public void putWithConditionTag() {
    putWithCondition(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  public void putWithConditionCommitMetadata() {
    putWithCondition(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  public void putWithConditionKeyFragment() {
    putWithCondition(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  public void putWithConditionL1() {
    putWithCondition(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  public void putWithConditionL2() {
    putWithCondition(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  public void putWithConditionL3() {
    putWithCondition(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  public void putToCorrectTagCollection() {
    final InternalRef sample = SampleEntities.createTag(random);
    putThenLoad(sample, ValueType.REF);
    final Id id = sample.getId();
    final InternalRef.Type type = InternalRef.Type.TAG;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), id.toEntity())
    );
    putConditional(sample, ValueType.REF, true, Optional.of(condition));
  }

  @Test
  public void putToCorrectBranchCollection() {
    final InternalRef sample = SampleEntities.createBranch(random);
    putThenLoad(sample, ValueType.REF);
    final Id id = sample.getId();
    final InternalRef.Type type = InternalRef.Type.BRANCH;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), id.toEntity()));
    putConditional(sample, ValueType.REF, true, Optional.of(condition));
  }

  @Test
  public void putFailingConditionExpression() {
    final InternalRef sample = SampleEntities.createBranch(random);
    final InternalRef.Type type = InternalRef.Type.BRANCH;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("name").build(), NON_EXISTENT));
    putConditional(sample, ValueType.REF, false, Optional.of(condition));
  }

  @Test
  public void putToIncorrectCollection() {
    // Trying to put an entity to the wrong collection should fail.
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.REF) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createTag(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.REF) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createTag(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.VALUE) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createValue(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.COMMIT_METADATA) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createCommitMetadata(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.KEY_FRAGMENT) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createFragment(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.L1) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createL1(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.L2) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createL2(random), Optional.empty()));
      }
    }
    for (ValueType value : ValueType.values()) {
      if (value != ValueType.L3) {
        assertThrows(IllegalArgumentException.class, () -> store.put(value, SampleEntities.createL3(random), Optional.empty()));
      }
    }
  }

  @Test
  public void deleteNoConditionalValue() {
    deleteConditional(SampleEntities.createValue(random), ValueType.VALUE, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalL1() {
    deleteConditional(SampleEntities.createL1(random), ValueType.L1, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalL2() {
    deleteConditional(SampleEntities.createL2(random), ValueType.L2, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalL3() {
    deleteConditional(SampleEntities.createL3(random), ValueType.L3, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalFragment() {
    deleteConditional(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalBranch() {
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalTag() {
    deleteConditional(SampleEntities.createTag(random), ValueType.REF, true, Optional.empty());
  }

  @Test
  public void deleteNoConditionalCommitMetadata() {
    deleteConditional(
        SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA, true, Optional.empty());
  }

  @Test
  public void deleteConditionalValue() {
    deleteConditional(SampleEntities.createValue(random), ValueType.VALUE, true, Optional.empty());
  }

  @Test
  public void deleteConditionalMismatchAttributeValue() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("value").build(), NON_EXISTENT);
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteConditional(SampleEntities.createValue(random), ValueType.VALUE, false, Optional.of(ex));
  }

  @Test
  public void deleteConditionalMismatchAttributeL1() {
    final ExpressionFunction expressionFunction =
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), NON_EXISTENT);
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, false, Optional.of(ex));
  }

  /**
   * This test creates a branch which has two commits in the history.
   * The test is to try to delete the branch specifying the commit history size should be one.
   * The delete should fail.
   */
  @Test
  public void deleteSelectedByConditionExpression1NoAlias() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), ONE));
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, false, Optional.of(expression));
  }

  /**
   * This test creates a branch which has two commits in the history.
   * The test is to try to delete the branch specifying the commit history size should be two.
   * The delete should succeed.
   */
  @Test
  public void deleteSelectedByConditionExpression2NoAlias() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), TWO));
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, true, Optional.of(expression));
  }

  private <T extends HasId> void putWithCondition(T sample, ValueType type) {
    // Tests that attempting to put (update) an existing entry should only occur when the condition expression is met.
    putThenLoad(sample, type);

    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    final Entity id  = Entity.ofBinary(sample.getId().toBytes());
    final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, id));
    putConditional(sample, type, true, Optional.of(conditionExpression));
  }

  protected <T extends HasId> void deleteConditional(T sample, ValueType type, boolean deleteSucceeded,
                                                   Optional<ConditionExpression> conditionExpression) {
    testPut(sample, type, Optional.empty());
    if (deleteSucceeded) {
      testDelete(type, sample.getId(), conditionExpression);
      testNotLoaded(sample, type);
    } else {
      testFailedDelete(type, sample.getId(), conditionExpression);
      testLoad(sample, type);
    }
  }

  protected <T extends HasId> void putConditional(T sample, ValueType type, boolean putSucceeded,
                                                  Optional<ConditionExpression> conditionExpression) {
    store.put(type, sample, conditionExpression);
    if (putSucceeded) {
      testLoad(sample, type);
    } else {
      testNotLoaded(sample, type);
    }
  }

  private <T extends HasId> void putThenLoad(T sample, ValueType type) {
    try {
      store.put(type, sample, Optional.empty());
      testLoad(sample, type);
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  private <T extends HasId> void testLoad(T sample, ValueType type) {
    try {
      final T read = store.loadSingle(type, sample.getId());
      final SimpleSchema<T> schema = type.getSchema();
      assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  private <T extends HasId> void testNotLoaded(T sample, ValueType type) {
    try {
      final T read = store.loadSingle(type, sample.getId());
      final SimpleSchema<T> schema = type.getSchema();
      schema.itemToMap(read, true);
      Assertions.fail();
    } catch (NotFoundException e) {
      // Exception is expected.
    }
  }

  private <T extends HasId> void testPutIfAbsent(T sample, ValueType type) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoad(sample, type);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoad(sample, type);
  }

  private <T extends HasId> void testPut(T sample, ValueType type, Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(type, sample, conditionExpression);
      testLoad(sample, type);
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  private void testDelete(ValueType type, Id id, Optional<ConditionExpression> conditionExpression) {
    Assertions.assertTrue(store.delete(type, id, conditionExpression));
  }

  private void testFailedDelete(ValueType type, Id id, Optional<ConditionExpression> conditionExpression) {
    Assertions.assertFalse(store.delete(type, id, conditionExpression));
  }
}
