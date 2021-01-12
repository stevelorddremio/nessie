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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperties;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * Common class for testing public APIs of a Store.
 * This class should be moved to the versioned/tests project when it will not introduce a circular dependency.
 * @param <S> The type of the Store being tested.
 */
public abstract class AbstractTestStore<S extends Store> {
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
  public void load() throws NotFoundException {
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.of(
        ValueType.REF, SampleEntities.createBranch(random),
        ValueType.REF, SampleEntities.createBranch(random),
        ValueType.L1, SampleEntities.createL1(random),
        ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
    for (Map.Entry<ValueType, HasId> entry : objs.entries()) {
      store.put(entry.getKey(), entry.getValue(), Optional.empty());
    }

    final LoadStep loadStep = new LoadStep(
        objs.entries().stream().map(e -> new LoadOp<>(e.getKey(), e.getValue().getId(),
            r -> assertEquals(e.getKey().getSchema().itemToMap(e.getValue(), true),
                e.getKey().getSchema().itemToMap(r, true)))
        ).collect(Collectors.toList())
    );

    store.load(loadStep);
  }

  @Test
  public void loadNone() throws NotFoundException {
    final LoadStep loadStep = new LoadStep(ImmutableList.of());
    store.load(loadStep);
  }

  @Test
  public void loadInvalid() throws NotFoundException {
    final Multimap<ValueType, HasId> objs = LinkedListMultimap.create();
    objs.put(ValueType.REF, SampleEntities.createBranch(random));
    for (Map.Entry<ValueType, HasId> entry : objs.entries()) {
      store.put(entry.getKey(), entry.getValue(), Optional.empty());
    }
    objs.put(ValueType.REF, SampleEntities.createBranch(random));

    final LoadStep loadStep = new LoadStep(
        objs.entries().stream().map(e -> new LoadOp<>(e.getKey(), e.getValue().getId(),
            r -> assertEquals(e.getKey().getSchema().itemToMap(e.getValue(), true),
                e.getKey().getSchema().itemToMap(r, true)))
        ).collect(Collectors.toList())
    );

    Assertions.assertThrows(NotFoundException.class, () -> store.load(loadStep));
  }

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

  // TODO: reenable test. This passes in parent branch.
  @Test
  @Disabled
  public void getRefs() {
    final List<InternalRef> expected = ImmutableList.of(SampleEntities.createBranch(random), SampleEntities.createTag(random));
    expected.forEach(e -> putThenLoad(e, ValueType.REF));

    // Ensure a consistent ordering by using a sort.
    final List<InternalRef> actual = store.getRefs().sorted(Comparator.comparing(o -> o.getId().toString())).collect(Collectors.toList());

    Assertions.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
      assertEquals(
          InternalRef.SCHEMA.itemToMap(expected.get(i), true),
          InternalRef.SCHEMA.itemToMap(actual.get(i), true));
    }
  }

  @Test
  public void getRefsNone() {
    Assertions.assertEquals(0, store.getRefs().count());
  }

  @Test
  public void save() {
    final L1 l1 = SampleEntities.createL1(random);
    final L2 l2 = SampleEntities.createL2(random);
    final InternalRef branch = SampleEntities.createBranch(random);
    final InternalRef tag = SampleEntities.createTag(random);
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, l1),
        new SaveOp<>(ValueType.L2, l2),
// TODO: Check store.save to allow multiple entries of same ValueType to be saved.
//        new SaveOp<>(ValueType.REF, branch),
        new SaveOp<>(ValueType.REF, tag)
    );
    store.save(saveOps);

    saveOps.forEach(s -> {
      final SimpleSchema<Object> schema = s.getType().getSchema();
      assertEquals(
          schema.itemToMap(s.getValue(), true),
          schema.itemToMap(store.loadSingle(s.getType(), s.getValue().getId()), true));
    });
  }

  void createSaveThread(int threadNumber, ArrayList<List<SaveOp<?>>> combinedSaveOps, ArrayList<Thread> threads,
                        CountDownLatch doneSignal) {
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, SampleEntities.createL1(random)),
        new SaveOp<>(ValueType.L2, SampleEntities.createL2(random)),
        new SaveOp<>(ValueType.L3, SampleEntities.createL3(random)),
        new SaveOp<>(ValueType.REF, SampleEntities.createBranch(random)),
//      new SaveOp<>(ValueType.REF, SampleEntities.createTag(random)),
        new SaveOp<>(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random)),
        new SaveOp<>(ValueType.VALUE, SampleEntities.createValue(random)),
        new SaveOp<>(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random))
    );

    combinedSaveOps.add(threadNumber, saveOps);

    class Worker implements Runnable {
      final List<SaveOp<?>> saveOps;

      Worker(List<SaveOp<?>> saveOps) {
        this.saveOps = saveOps;
      }

      @Override
      public void run() {
        store.save(saveOps);
        doneSignal.countDown();
      }
    }
    threads.add(new Thread(new Worker(saveOps)));
  }

  @Test
  public void saveMultithreaded() {

    final int threadCount = 100;
    CountDownLatch doneSignal = new CountDownLatch(threadCount);
    ArrayList<Thread> threads = new ArrayList<>();
    ArrayList<List<SaveOp<?>>> combinedSaveOps = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      createSaveThread(i, combinedSaveOps, threads, doneSignal);
    }

    for (Thread thread : threads) {
      thread.start();
    }

    try {
      doneSignal.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      fail();
    }

    for (List<SaveOp<?>> combinedSaveOp : combinedSaveOps) {
      combinedSaveOp.forEach(s -> {
        final SimpleSchema<Object> schema = s.getType().getSchema();
        assertEquals(
            schema.itemToMap(s.getValue(), true),
            schema.itemToMap(store.loadSingle(s.getType(), s.getValue().getId()), true));
      });
    }
  }

  private <T extends HasId> void putThenLoad(T sample, ValueType type) {
    store.put(type, sample, Optional.empty());
    testLoad(sample, type);
  }

  private <T extends HasId> void testLoad(T sample, ValueType type) {
    final T read = store.loadSingle(type, sample.getId());
    final SimpleSchema<T> schema = type.getSchema();
    assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
  }

  private <T extends HasId> void testPutIfAbsent(T sample, ValueType type) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoad(sample, type);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoad(sample, type);
  }
}
