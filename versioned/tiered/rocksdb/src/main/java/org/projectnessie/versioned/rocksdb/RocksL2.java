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

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.UpdateClause;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L2;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L2} providing
 * SerDe and Condition evaluation.
 */
class RocksL2 extends RocksBaseValue<L2> implements L2 {
  private static final String CHILDREN = "children";

  private final ValueProtos.L2.Builder builder = ValueProtos.L2.newBuilder();

  RocksL2() {
    super();
  }

  @Override
  public L2 children(Stream<Id> ids) {
    builder
        .clearTree()
        .addAllTree(ids.map(Id::getValue).collect(Collectors.toList()));
    return this;
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case CHILDREN:
        evaluate(function, builder.getTreeList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }

  }

  @Override
  public boolean updateWithClause(UpdateClause updateClause) {
    final UpdateFunction function = updateClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
    final ExpressionPath.NameSegment nameSegment = function.getRootPathAsNameSegment();
    final String segment = nameSegment.getName();

    switch (segment) {
      case ID:
        if (function.getOperator() == UpdateFunction.Operator.SET) {
          UpdateFunction.SetFunction setFunction = (UpdateFunction.SetFunction) function;
          if (setFunction.getSubOperator().equals(UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST)) {
            throw new UnsupportedOperationException();
          } else if (setFunction.getSubOperator().equals(UpdateFunction.SetFunction.SubOperator.EQUALS)) {
            id(Id.of(setFunction.getValue().getBinary()));
          }
        } else {
          throw new UnsupportedOperationException();
        }
        break;
      case CHILDREN:
        updateByteStringList(
            function,
            l2Builder::getTreeList,
            l2Builder::addTree,
            l2Builder::addAllTree,
            l2Builder::clearTree,
            l2Builder::setTree
        );
        break;
    }

    return true;
  }

  @Override
  byte[] build() {
    checkPresent(builder.getTreeList(), CHILDREN);

    return builder.setBase(buildBase()).build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L2 consumer) {
    try {
      final ValueProtos.L2 l2 = ValueProtos.L2.parseFrom(value);
      setBase(consumer, l2.getBase());
      consumer.children(l2.getTreeList().stream().map(Id::of));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L2 value encountered when deserializing.", e);
    }
  }

  Stream<Id> getChildren() {
    return l2Builder.getTreeList().stream().map(Id::of);
  }
}
