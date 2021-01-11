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
package com.dremio.nessie.versioned.store.rocksdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.tests.AbstractTestStore;
import com.google.common.collect.ImmutableList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestRocksDBStore extends AbstractTestStore<RocksDBStore> {
  @TempDir
  static Path DB_PATH;

  @AfterAll
  static void tearDown() throws IOException {
    if (DB_PATH.toFile().exists()) {
      Files.walk(DB_PATH).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  /**
   * Creates an instance of RocksDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected RocksDBStore createStore() {
    return new RocksDBStore(DB_PATH.toString());
  }

  @Override
  protected long getRandomSeed() {
    return -2938423452345L;
  }

  @Override
  protected void resetStoreState() {
    store.deleteAllData();
  }

  void createSaveThread(int threadNumber, ArrayList<List<SaveOp<?>>> combinedSaveOps, ArrayList<Thread> threads, CountDownLatch doneSignal) {
    final List<SaveOp<?>> saveOps = ImmutableList.of(
      new SaveOp<>(ValueType.L1, SampleEntities.createL1(random)),
      new SaveOp<>(ValueType.L2, SampleEntities.createL2(random)),
      new SaveOp<>(ValueType.L3, SampleEntities.createL3(random)),
      new SaveOp<>(ValueType.REF, SampleEntities.createBranch(random)),
      new SaveOp<>(ValueType.REF, SampleEntities.createTag(random)),
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
      doneSignal.await(100, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {}

    for (List<SaveOp<?>> combinedSaveOp : combinedSaveOps) {
      combinedSaveOp.forEach(s -> {
        final SimpleSchema<Object> schema = s.getType().getSchema();
        assertEquals(
          schema.itemToMap(s.getValue(), true),
          schema.itemToMap(store.loadSingle(s.getType(), s.getValue().getId()), true));
      });
    }
  }

  @Override
  @Test
  public void putIfAbsentBranch() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentBranch());
  }

  @Override
  public void putIfAbsentTag() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentTag());
  }

  @Override
  public void putIfAbsentValue() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentValue());
  }

  @Override
  public void putIfAbsentCommitMetadata() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentCommitMetadata());
  }

  @Override
  public void putIfAbsentFragment() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentFragment());
  }

  @Override
  public void putIfAbsentL1() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL1());
  }

  @Override
  public void putIfAbsentL2() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL2());
  }

  @Override
  public void putIfAbsentL3() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL3());
  }

}
