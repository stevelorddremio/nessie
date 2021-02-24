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

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Id;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksL2 update() tests")
public class TestUpdateFunctionRocksL2 extends TestUpdateFunctionBase {
  final RocksL2 rocksL2 = createL2(RANDOM);

  /**
   * Create a Sample L1 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  static RocksL2 createL2(Random random) {
    return (RocksL2) new RocksL2()
      .id(Id.EMPTY)
      .children(Stream.generate(() -> ID).limit(RocksL1.SIZE));
  }

  @Test
  void idRemove() {
    idRemove(rocksL2);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksL2);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksL2);
  }

  @Test
  void childrenRemove() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(
        RemoveClause.of(ExpressionPath.builder(RocksL1.CHILDREN).build()));
    updateTestFails(rocksL2, updateExpression);
  }

  @Test
  void childrenSetEquals() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_ENTITY_LIST));
    rocksL2.update(updateExpression);
    final List<Id> updatedList = rocksL2.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(ID_LIST, updatedList);
  }

  @Test
  void childrenSetAppendToListWithId() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_2.toEntity()));
    final List<Id> initialList = rocksL2.getChildren().collect(Collectors.toList());
    rocksL2.update(updateExpression);
    final List<Id> updatedList = rocksL2.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), 1);
    Assertions.assertEquals(ID_2, updatedList.get(updatedList.size() - 1));
  }

  @Test
  void childrenSetAppendToListWithList() {
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksL1.CHILDREN).build(), ID_ENTITY_LIST));
    final List<Id> initialList = rocksL2.getChildren().collect(Collectors.toList());
    rocksL2.update(updateExpression);
    final List<Id> updatedList = rocksL2.getChildren().collect(Collectors.toList());
    Assertions.assertEquals(updatedList.size() - initialList.size(), ID_LIST.size());
    Assertions.assertEquals(ID_LIST, updatedList.subList(initialList.size(), updatedList.size()));
  }
}
