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
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L3;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L3} providing
 * SerDe and Condition evaluation.
 *
 * <p>Conceptually, this is matching the following JSON structure:</p>
 * <pre>{
 *   "id": &lt;ByteString&gt;,    // ID
 *   "dt": &lt;int64&gt;,         // DATETIME
 *   "tree": [                    // TREE
 *     {
 *       "key": &lt;String&gt;,   // TREE_KEY
 *       "id": &lt;ByteString&gt; // TREE_ID
 *     }
 *   ]
 * }</pre>
 */
class RocksL3 extends RocksBaseValue<L3> implements L3 {
  static final String TREE = "tree";
  static final String TREE_KEY = "key";
  static final String TREE_ID = "id";

  private final ValueProtos.L3.Builder l3Builder = ValueProtos.L3.newBuilder();

  RocksL3() {
    super();
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    l3Builder.clearKeyDelta();

    keyDelta.forEach(kd -> {
      final ValueProtos.KeyDelta.Builder keyDeltaBuilder = ValueProtos.KeyDelta.newBuilder();
      keyDeltaBuilder.setId(kd.getId().getValue());
      keyDeltaBuilder.setKey(ValueProtos.Key.newBuilder().addAllElements(kd.getKey().getElements()));

      l3Builder.addKeyDelta(keyDeltaBuilder);
    });

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
  protected void remove(ExpressionPath path) {
    // tree[*]
    if (path.accept(PathPattern.exact(TREE).anyPosition())) {
      final List<ValueProtos.KeyDelta> updatedKeyDeltas = new ArrayList<>(l3Builder.getKeyDeltaList());
      updatedKeyDeltas.remove(getPathSegmentAsPosition(path, 1));
      l3Builder.clearKeyDelta().addAllKeyDelta(updatedKeyDeltas);
    // tree[*]/key[*]
    } else if (path.accept(PathPattern.exact(TREE).anyPosition().nameEquals(TREE_KEY).anyPosition())) {
      final int keyDeltaPosition = getPathSegmentAsPosition(path, 1);
      final int keyPosition = getPathSegmentAsPosition(path, 3);

      final List<String> updatedKeys = new ArrayList<>(l3Builder.getKeyDelta(keyDeltaPosition).getKey().getElementsList());
      updatedKeys.remove(keyPosition);

      final ValueProtos.KeyDelta updatedKeyDelta = ValueProtos.KeyDelta
          .newBuilder()
          .setId(l3Builder.getKeyDelta(keyDeltaPosition).getId())
          .setKey(ValueProtos.Key.newBuilder().addAllElements(updatedKeys))
          .build();

      l3Builder.clearKeyDelta().setKeyDelta(keyDeltaPosition, updatedKeyDelta);
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for remove in L3", path.asString()));
    }
  }

  @Override
  protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
    if (path.accept(PathPattern.exact(TREE))) {
      valuesToAdd.forEach(e -> l3Builder.addKeyDelta(EntityConverter.entityToKeyDelta(e)));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition().nameEquals(TREE_KEY))) {
      final int treePosition = getPathSegmentAsPosition(path, 1);

      final ValueProtos.Key updatedKey = ValueProtos.Key
          .newBuilder(l3Builder.getKeyDelta(treePosition).getKey())
          .addAllElements(valuesToAdd.stream().map(Entity::toString).collect(Collectors.toList()))
          .build();

      l3Builder.setKeyDelta(treePosition, ValueProtos.KeyDelta.newBuilder(l3Builder.getKeyDelta(treePosition)).setKey(updatedKey));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for append in L3", path.asString()));
    }
  }

  @Override
  protected void set(ExpressionPath path, Entity newValue) {
    if (path.accept(PathPattern.exact(TREE))) {
      l3Builder.clearKeyDelta();
      newValue.getList().forEach(e -> l3Builder.addKeyDelta(EntityConverter.entityToKeyDelta(e)));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition())) {
      l3Builder.setKeyDelta(getPathSegmentAsPosition(path, 1), EntityConverter.entityToKeyDelta(newValue));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition().nameEquals(TREE_ID))) {
      final int i = getPathSegmentAsPosition(path, 1);
      l3Builder.setKeyDelta(i, ValueProtos.KeyDelta.newBuilder(l3Builder.getKeyDelta(i)).setId(newValue.getBinary()));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition().nameEquals(TREE_KEY))) {
      final int i = getPathSegmentAsPosition(path,1);

      l3Builder.setKeyDelta(i, ValueProtos.KeyDelta
          .newBuilder(l3Builder.getKeyDelta(i))
          .setKey(EntityConverter.entityToKey(newValue)));
    } else if (path.accept(PathPattern.exact(TREE).anyPosition().nameEquals(TREE_KEY).anyPosition())) {
      final int keyDeltaIndex = getPathSegmentAsPosition(path, 1);
      final int keyIndex = getPathSegmentAsPosition(path, 3);

      final List<String> updatedKeyValues = new ArrayList<>(l3Builder.getKeyDelta(keyDeltaIndex).getKey().getElementsList());
      updatedKeyValues.set(keyIndex, newValue.getString());

      l3Builder.setKeyDelta(keyDeltaIndex, ValueProtos.KeyDelta
          .newBuilder(l3Builder.getKeyDelta(keyDeltaIndex))
          .setKey(ValueProtos.Key.newBuilder().addAllElements(updatedKeyValues)));
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for set equals in L3", path.asString()));
    }
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
