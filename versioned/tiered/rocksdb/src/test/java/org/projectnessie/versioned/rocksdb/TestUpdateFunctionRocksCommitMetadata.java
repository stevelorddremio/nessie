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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

import com.google.protobuf.ByteString;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("RocksValue update() tests")
public class TestUpdateFunctionRocksCommitMetadata extends TestUpdateFunctionBase {
  final RocksCommitMetadata rocksCommitMetadata = createCommitMetadata(RANDOM);

  /**
   * Create a Sample CommitMetadata entity.
   * @param random object to use for randomization of entity creation.
   * @return sample CommitMetadata entity.
   */
  static RocksCommitMetadata createCommitMetadata(Random random) {
    return (RocksCommitMetadata) new RocksCommitMetadata()
      .id(Id.EMPTY)
      .value(createRandomByteString(random));
  }

  static ByteString createRandomByteString(Random random) {
    return ByteString.copyFrom(SampleEntities.createBinary(random, 20));
  }

  @Test
  void idRemove() {
    idRemove(rocksCommitMetadata);
  }

  @Test
  void idSetEquals() {
    idSetEquals(rocksCommitMetadata);
  }

  @Test
  void idSetAppendToList() {
    idSetAppendToList(rocksCommitMetadata);
  }

  @Test
  void valueRemove() {
    final UpdateExpression updateExpression = UpdateExpression.of(RemoveClause.of(ExpressionPath.builder(RocksWrappedValue.VALUE).build()));
    updateTestFails(rocksCommitMetadata, updateExpression);
  }

  @Test
  void valueSetEquals() {
    final ByteString newValue = createRandomByteString(RANDOM);
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder(RocksWrappedValue.VALUE).build(), Entity.ofBinary(newValue)));
    rocksCommitMetadata.update(updateExpression);
    Assertions.assertEquals(newValue, rocksCommitMetadata.getValue());
  }

  @Test
  void valueSetAppendToList() {
    final Id newId = SampleEntities.createId(RANDOM);
    final UpdateExpression updateExpression =
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder(RocksWrappedValue.VALUE).build(), newId.toEntity()));
    updateTestFails(rocksCommitMetadata, updateExpression);
  }
}
