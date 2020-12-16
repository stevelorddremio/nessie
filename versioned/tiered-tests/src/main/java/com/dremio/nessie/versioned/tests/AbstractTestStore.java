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
  private Random random;
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
  public void putValue() {
    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    HasId value = SampleEntities.createValue(random);
    final Entity id  = Entity.ofString(value.getId().toString());
    ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, id));
    testPut(value, ValueType.VALUE, Optional.of(conditionExpression));
  }

  @Test
  public void deleteNoConditions() {
    HasId value = SampleEntities.createValue(random);
    testPut(value, ValueType.VALUE, Optional.empty());
    testDelete(ValueType.VALUE, (value).getId(), Optional.empty());
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

  private <T extends HasId> void testPutIfAbsent(T sample, ValueType type) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoad(sample, type);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoad(sample, type);
  }

  private <T extends HasId> void testPut(T sample, ValueType type, Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(type, sample, conditionExpression);
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
    testLoad(sample, type);
  }

  private void testDelete(ValueType type, Id id, Optional<ConditionExpression> conditionExpression) {
    Assertions.assertTrue(store.delete(type, id, conditionExpression));
  }
}
