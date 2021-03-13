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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.store.Id;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksFragment update() tests")
public class TestUpdateFunctionRocksFragment extends TestUpdateFunctionBase {
  final RocksFragment rocksFragment = createFragment();

  /**
   * Create a Sample Fragment entity.
   * @return sample Fragment entity.
   */
  static RocksFragment createFragment() {
    return (RocksFragment) new RocksFragment()
      .id(Id.EMPTY);
  }

  @Test
  void idRemove() {
    idRemove(rocksFragment);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksFragment);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksFragment);
  }
}
