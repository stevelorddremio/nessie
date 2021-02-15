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
package org.projectnessie.versioned.mongodb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

@ExtendWith(LocalMongo.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("MongoDBStore not fully implemented")
public class TestMongoDBVersionStore extends AbstractITVersionStore {
  private String connectionString;

  private MongoStoreFixture fixture;

  @BeforeAll
  void init(String connectionString) {
    this.connectionString = connectionString;
  }

  @BeforeEach
  void setup() {
    fixture = new MongoStoreFixture(connectionString);
  }

  @AfterEach
  void deleteResources() {
    fixture.close();
  }

  @Override
  protected VersionStore<String, String> store() {
    return fixture;
  }

  @Disabled
  @Override
  public void commitWithInvalidReference() throws ReferenceNotFoundException,
      ReferenceConflictException, ReferenceAlreadyExistsException {
    super.commitWithInvalidReference();
  }

  @Nested
  @DisplayName("when transplanting")
  class WhenTransplanting extends AbstractITVersionStore.WhenTransplanting {
    @Disabled
    @Override
    protected void checkInvalidBranchHash() throws VersionStoreException {
      super.checkInvalidBranchHash();
    }
  }
}
