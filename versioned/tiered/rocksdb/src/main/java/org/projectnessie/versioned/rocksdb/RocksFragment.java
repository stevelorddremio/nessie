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

import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.Fragment;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.Fragment} providing
 * SerDe and Condition evaluation.
 *
 * <p>Conceptually, this is matching the following JSON structure:</p>
 * <pre>{
 *   "id": &lt;ByteString&gt;, // ID
 *   "dt": &lt;int64&gt;,      // DATETIME
 *   "keys": [                 // KEY_LIST
 *     [
 *       &lt;String&gt;
 *     ]
 *   ]
 * }</pre>
 */
class RocksFragment extends RocksBaseValue<Fragment> implements Fragment {
  static final String KEY_LIST = "keys";

  private final ValueProtos.Fragment.Builder fragmentBuilder = ValueProtos.Fragment.newBuilder();

  RocksFragment() {
    super();
  }

  @Override
  public Fragment keys(Stream<Key> keys) {
    fragmentBuilder
        .clearKeys()
        .addAllKeys(keys.map(key -> ValueProtos.Key
            .newBuilder()
            .addAllElements(key.getElements())
            .build()
        )
        .collect(Collectors.toList()));
    return this;
  }

  @Override
  byte[] build() {
    checkPresent(fragmentBuilder.getKeysList(), KEY_LIST);

    return fragmentBuilder.setBase(buildBase()).build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, Fragment consumer) {
    try {
      final ValueProtos.Fragment fragment = ValueProtos.Fragment.parseFrom(value);
      setBase(consumer, fragment.getBase());
      consumer.keys(fragment.getKeysList().stream().map(RocksBaseValue::createKey));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Fragment value encountered when deserializing.", e);
    }
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case KEY_LIST:
        if (function.getRootPathAsNameSegment().getChild().isPresent()) {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        } else if (function.getOperator().equals(Function.Operator.EQUALS)) {
          if (!keysAsEntityList(getKeys()).equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else if (function.getOperator().equals(Function.Operator.SIZE)) {
          if (fragmentBuilder.getKeysCount() != function.getValue().getNumber()) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        break;
      default:
        // NameSegment could not be applied to FunctionExpression.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  private List<Key> getKeys() {
    return fragmentBuilder.getKeysList()
      .stream()
      .map(key ->
        ImmutableKey
          .builder()
          .addAllElements(new ArrayList<>(key.getElementsList()))
          .build()
      )
      .collect(Collectors.toList());
  }

  /**
   * Produces an Entity List of Entity Lists for keys.
   * Each key is represented as an entity list of string entities.
   * @param keys stream of keys to convert
   * @return an entity list of keys
   */
  private Entity keysAsEntityList(List<Key> keys) {
    return Entity.ofList(keys.stream().map(k -> Entity.ofList(k.getElements().stream().map(Entity::ofString))));
  }

  @Override
  protected void remove(String fieldName, ExpressionPath.PathSegment path) {
    final PathPattern keyItemPath = new PathPattern().anyPosition();
    final PathPattern keyInnerItemPath = new PathPattern().anyPosition().anyPosition();

    if (keyItemPath.matches(path)) {
      final int i = path.asPosition().getPosition();
      fragmentBuilder.removeKeys(i);
    } else if (keyInnerItemPath.matches(path)) {
      final int outerIndex = path.asPosition().getPosition();
      final int innerIndex = path.getChild().get().asPosition().getPosition();

      final List<String> updatedKeys = new ArrayList<>(fragmentBuilder.getKeys(outerIndex).getElementsList());
      updatedKeys.remove(innerIndex);

      fragmentBuilder.setKeys(path.asPosition().getPosition(), ValueProtos.Key
          .newBuilder()
          .addAllElements(updatedKeys)
      );
    } else {
      throw new UnsupportedOperationException("Invalid path for remove");
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    if (!KEY_LIST.equals(fieldName)) {
      return false;
    }

    return new PathPattern().matches(childPath) || new PathPattern().anyPosition().matches(childPath);
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    final PathPattern keyPath = new PathPattern();
    final PathPattern keyItemPath = new PathPattern().anyPosition();

    if (keyPath.matches(childPath)) {
      fragmentBuilder.addAllKeys(valuesToAdd.stream().map(v -> ValueProtos.Key
          .newBuilder()
          .addAllElements(v.getList().stream().map(Entity::getString).collect(Collectors.toList()))
          .build()
      ).collect(Collectors.toList()));
    } else if (keyItemPath.matches(childPath)) {
      final int i = childPath.asPosition().getPosition();

      fragmentBuilder.setKeys(i, ValueProtos.Key
          .newBuilder()
          .addAllElements(fragmentBuilder.getKeys(i).getElementsList())
          .addAllElements(valuesToAdd.stream().map(Entity::getString).collect(Collectors.toList()))
      );
    } else {
      throw new UnsupportedOperationException("Invalid path for append");
    }
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    final PathPattern keyPath = new PathPattern();
    final PathPattern keyItemPath = new PathPattern().anyPosition();
    final PathPattern keyInnerItemPath = new PathPattern().anyPosition().anyPosition();

    if (keyPath.matches(childPath)) {
      fragmentBuilder.addAllKeys(newValue.getList()
          .stream()
          .map(keys -> ValueProtos.Key
              .newBuilder()
              .addAllElements(keys.getList().stream().map(Entity::getString).collect(Collectors.toList()))
              .build())
          .collect(Collectors.toList()));
    } else if (keyItemPath.matches(childPath)) {
      final int i = childPath.asPosition().getPosition();

      fragmentBuilder.setKeys(i, ValueProtos.Key
          .newBuilder()
          .addAllElements(newValue.getList().stream().map(Entity::getString).collect(Collectors.toList())));
    } else if (keyInnerItemPath.matches(childPath)) {
      final int outerIndex = childPath.asPosition().getPosition();
      final int innerIndex = childPath.getChild().get().asPosition().getPosition();

      final List<String> updatedKeys = new ArrayList<>(fragmentBuilder.getKeys(outerIndex).getElementsList());
      updatedKeys.remove(innerIndex);

      fragmentBuilder.setKeys(outerIndex, ValueProtos.Key.newBuilder().addAllElements(updatedKeys));
    } else {
      throw new UnsupportedOperationException("Invalid path for set equals");
    }
  }
}
