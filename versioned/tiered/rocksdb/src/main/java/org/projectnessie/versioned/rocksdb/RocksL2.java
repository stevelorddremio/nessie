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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L2;

import com.google.protobuf.ByteString;
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
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    if (CHILDREN.equals(fieldName)) {
      List<ByteString> updatedChildren = new ArrayList<>(l2Builder.getTreeList());
      updatedChildren.remove(getPosition(path));
      l2Builder.clearTree().addAllTree(updatedChildren);
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    return CHILDREN.equals(fieldName) && childPath == null;
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    if (CHILDREN.equals(fieldName)) {
      l2Builder.addAllTree(valuesToAdd.stream().map(Entity::getBinary).collect(Collectors.toList()));
    }
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    if (CHILDREN.equals(fieldName)) {
      if (childPath != null) {
        l2Builder.setTree(getPosition(childPath), newValue.getBinary());
      } else {
        l2Builder.clearTree().addAllTree(newValue.getList().stream().map(Entity::getBinary).collect(Collectors.toList()));
      }
    }
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
