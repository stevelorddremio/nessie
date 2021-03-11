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
 *
 * <p>Conceptually, this is matching the following JSON structure:</p>
 * <pre>{
 *   "id": &lt;ByteString&gt;, // ID
 *   "dt": &lt;int64&gt;,      // DATETIME
 *   "tree": [                 // TREE
 *     &lt;ByteString&gt;
 *   ]
 * }</pre>
 */
class RocksL2 extends RocksBaseValue<L2> implements L2 {
  static final String TREE = "tree";

  private final ValueProtos.L2.Builder l2Builder = ValueProtos.L2.newBuilder();

  RocksL2() {
    super();
  }

  @Override
  public L2 children(Stream<Id> ids) {
    l2Builder.clearTree();
    ids.forEach(id -> l2Builder.addTree(id.getValue()));
    return this;
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case TREE:
        evaluate(function, l2Builder.getTreeList().stream().map(Id::of).collect(Collectors.toList()));
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }

  }

  @Override
  protected void remove(ExpressionPath path) {
    if (path.accept(PathPattern.exact(TREE).anyPosition())) {
      List<ByteString> updatedChildren = new ArrayList<>(l2Builder.getTreeList());
      updatedChildren.remove(getPathSegmentAsPosition(path, 1));

      l2Builder.clearTree();
      l2Builder.addAllTree(updatedChildren);
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for remove in L2", path.asString()));
    }
  }

  @Override
  protected boolean fieldIsList(ExpressionPath path) {
    return path.accept(PathPattern.exact(TREE));
  }

  @Override
  protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
    if (path.accept(PathPattern.exact(TREE))) {
      valuesToAdd.forEach(e -> l2Builder.addTree(e.getBinary()));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for append in L2", path.asString()));
    }
  }

  @Override
  protected void set(ExpressionPath path, Entity newValue) {
    if (path.accept(PathPattern.exact(TREE))) {
      l2Builder.clearTree();
      newValue.getList().forEach(e -> l2Builder.addTree(e.getBinary()));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition())) {
      l2Builder.setTree(getPathSegmentAsPosition(path, 1), newValue.getBinary());
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for set equals in L2", path.asString()));
    }
  }

  @Override
  byte[] build() {
    checkPresent(l2Builder.getTreeList(), TREE);

    return l2Builder.setBase(buildBase()).build().toByteArray();
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
