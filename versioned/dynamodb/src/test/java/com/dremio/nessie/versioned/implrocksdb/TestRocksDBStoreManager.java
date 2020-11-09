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
package com.dremio.nessie.versioned.implrocksdb;

//import static java.nio.charset.StandardCharsets.UTF_8;
//import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
//import org.rocksdb.ColumnFamilyDescriptor;
//import org.rocksdb.ColumnFamilyHandle;
//import org.rocksdb.RocksDBException;

public class TestRocksDBStoreManager {
  private RocksDBStoreManager rocksDBStoreManager;

  /**
   * Ensures that database is closed after each test.
   * @throws IOException the exception thrown
   */
  @AfterEach
  public void closeStore() throws IOException {
    if (rocksDBStoreManager != null) {
      try {
        rocksDBStoreManager.close();
      } catch (Exception e) {
        assert (false);
      }
    }
  }

  @Test
  public void createDefaultRocksDBManagerInstance() {
    String path = "/tmp/db";
    RocksDBStoreManager rocksDBStore = new RocksDBStoreManager(path, true);
    assertNotNull(rocksDBStore);
  }

  @Test
  public void startAndCloseInstance() {
    String path = "/tmp/db";
    rocksDBStoreManager = new RocksDBStoreManager(path, true);
    assertNotNull(rocksDBStoreManager);
    rocksDBStoreManager.start();
  }

  //@Disabled
  /*
  @Test
  public void findFamilyHandles() {
    String testColumnFamName = "testColumnFamName";
    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(testColumnFamName.getBytes(UTF_8));
    testColumnFamName.getBytes(UTF_8);
    String path = "/tmp/db";
    rocksDBStore = new RocksDBStore(testColumnFamName, new ColumnFamilyDescriptor::new, new ColumnFamilyHandle(), path);
    assertNotNull(rocksDBStore);
    rocksDBStore.start();

    ColumnFamilyHandle columnFamilyHandle = rocksDBStore.db.getDefaultColumnFamily();
    assertEquals(0, columnFamilyHandle.getID());

    final byte[] l3ColumnFamily = "l3".getBytes();
    byte[] key = null;

    try {
      int result = rocksDBStore.db.get(l3ColumnFamily, key);
      assertEquals(4, result);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }
  */
}
