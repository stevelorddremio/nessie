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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRocksDBVersionStore extends AbstractITVersionStore {
  private static final String testDatabaseName = "mydb";

  private RocksDBStoreFixture fixture;

  @BeforeEach
  void setup() {
    fixture = new RocksDBStoreFixture(testDatabaseName);
  }

  @AfterEach
  void deleteResources() {
    fixture.close();
  }

  @Override
  protected VersionStore<String, String> store() {
    return fixture;
  }

  @Nested
  @DisplayName("when transplanting")
  class WhenTransplanting extends AbstractITVersionStore.WhenTransplanting {
    @Override
    protected void checkInvalidBranchHash() throws VersionStoreException {
      super.checkInvalidBranchHash();
    }
  }
}
