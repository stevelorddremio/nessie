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

import com.dremio.nessie.tiered.builder.CommitMetadata;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

class RocksCommitMetadata extends RocksWrappedValue<CommitMetadata> implements Evaluator, CommitMetadata {
  static CommitMetadata of(Id id, long dt, ByteString value) {
    return new RocksCommitMetadata().id(id).dt(dt).value(value);
  }

  RocksCommitMetadata() {
    super();
  }

  @Override
  public boolean evaluate(Condition condition) {
    for (Function function: condition.getFunctionList()) {
      if (function.getPath().getRoot().isName()) {
        ExpressionPath.NameSegment nameSegment = function.getPath().getRoot().asName();
        final String segment = nameSegment.getName();

        switch (segment) {
          case ID:
            if (!(!nameSegment.getChild().isPresent()
                && function.getOperator().equals(Function.EQUALS)
                && getId().toEntity().equals(function.getValue()))) {
              return false;
            }
            break;
          case VALUE:
            if (!(!nameSegment.getChild().isPresent()
                && function.getOperator().equals(Function.EQUALS)
                && byteValue.toStringUtf8().equals(function.getValue().getString()))) {
              return false;
            }
            break;
          default:
            // Invalid Condition Function.
            return false;
        }
      }
    }
    // All functions have passed the test.
    return true;
  }
}
