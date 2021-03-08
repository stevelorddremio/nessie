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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L3;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L3} providing
 * SerDe and Condition evaluation.
 *
 * Conceptually, this is matching the following JSON structure:
 * {
 *   "id": &lt;ByteString&gt;, // ID
 *   "dt": &lt;int64&gt;,      // DATETIME
 *   "tree": [                 // TREE
 *     &lt;ByteString&gt;
 *   ]
 * }
 */
class RocksL3 extends RocksBaseValue<L3> implements L3 {
  private static final String TREE = "tree";

  private final ValueProtos.L3.Builder l3Builder = ValueProtos.L3.newBuilder();

  RocksL3() {
    super();
  }


  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    l3Builder
        .clearKeyDelta()
        .addAllKeyDelta(
            keyDelta.map(kd -> ValueProtos.KeyDelta
                .newBuilder()
                .setKey(ValueProtos.Key.newBuilder().addAllElements(kd.getKey().getElements()).build())
                .setId(kd.getId().getValue())
                .build()
            )
            .collect(Collectors.toList())
        );
    return this;
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    if (segment.equals(ID)) {
      evaluatesId(function);
    } else {
      throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    return TREE.equals(fieldName);
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  byte[] build() {
    checkPresent(l3Builder.getKeyDeltaList(), TREE);

    return l3Builder.setBase(buildBase()).build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L3 consumer) {
    try {
      final ValueProtos.L3 l3 = ValueProtos.L3.parseFrom(value);
      setBase(consumer, l3.getBase());
      consumer.keyDelta(l3.getKeyDeltaList()
          .stream()
          .map(d -> KeyDelta.of(createKey(d.getKey()), Id.of(d.getId()))));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L3 value encountered when deserializing.", e);
    }
  }
}
