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

import java.util.Random;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.store.Id;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksL3 update() tests")
public class TestUpdateFunctionRocksL3 extends TestUpdateFunctionBase {
  final RocksL3 rocksL3 = createL3(RANDOM);

  /**
   * Create a Sample L3 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L3 entity.
   */
  static RocksL3 createL3(Random random) {
    return (RocksL3) new RocksL3()
      .id(Id.EMPTY);
  }

  @Test
  void idRemove() {
    idRemove(rocksL3);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksL3);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksL3);
  }
}
