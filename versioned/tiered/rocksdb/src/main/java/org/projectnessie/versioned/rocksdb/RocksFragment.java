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
    if (KEY_LIST.equals(fieldName)) {
      if (path.isPosition()) {
        if (path.getChild().isPresent()) {
          if (path.getChild().get().isPosition()) {
            final List<String> updatedKeys = new ArrayList<>(fragmentBuilder.getKeys(path.asPosition().getPosition()).getElementsList());
            updatedKeys.remove(path.getChild().get().asPosition().getPosition());
            fragmentBuilder.setKeys(path.asPosition().getPosition(), ValueProtos.Key
                .newBuilder()
                .addAllElements(updatedKeys)
            );
          } else {
            throw new UnsupportedOperationException("Invalid path for remove");
          }
        } else {
          fragmentBuilder.removeKeys(path.asPosition().getPosition());
        }
      } else {
        throw new UnsupportedOperationException("Invalid path for remove");
      }
    } else {
      throw new UnsupportedOperationException(String.format("Unknown field \"%s\"", fieldName));
    }
  }

  @Override
  protected boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath) {
    return KEY_LIST.equals(fieldName);
  }

  @Override
  protected void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd) {
    if (KEY_LIST.equals(fieldName)) {
      if (childPath == null) {
        fragmentBuilder.addAllKeys(valuesToAdd.stream().map(v -> ValueProtos.Key
            .newBuilder()
            .addAllElements(v.getList().stream().map(Entity::getString).collect(Collectors.toList()))
            .build()
        ).collect(Collectors.toList()));
      } else if (childPath.isPosition()) {
        fragmentBuilder.setKeys(childPath.asPosition().getPosition(), ValueProtos.Key
            .newBuilder()
            .addAllElements(fragmentBuilder.getKeys(childPath.asPosition().getPosition()).getElementsList())
            .addAllElements(valuesToAdd.stream().map(Entity::getString).collect(Collectors.toList()))
        );
      } else {
        throw new UnsupportedOperationException("Invalid path for appending to \"keys\"");
      }
    } else {
      throw new UnsupportedOperationException(String.format("Unknown field \"%s\"", fieldName));
    }
  }

  @Override
  protected void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue) {
    if (KEY_LIST.equals(fieldName)) {
      if (childPath.isPosition()) {
        int index = childPath.asPosition().getPosition();

        if (childPath.getChild().isPresent() && childPath.getChild().get().isPosition()) {
          final List<String> updatedKeys = new ArrayList<>(fragmentBuilder.getKeys(index).getElementsList());
          updatedKeys.set(childPath.getChild().get().asPosition().getPosition(), newValue.getString());
          fragmentBuilder.setKeys(index, ValueProtos.Key
              .newBuilder()
              .addAllElements(updatedKeys)
          );
        } else if (!childPath.getChild().isPresent()) {
          fragmentBuilder.setKeys(index, ValueProtos.Key
              .newBuilder()
              .addAllElements(newValue.getList().stream().map(Entity::getString).collect(Collectors.toList()))
          );
        } else {
          throw new UnsupportedOperationException("Invalid path for set equals");
        }
      } else {
        throw new UnsupportedOperationException("Invalid path for set equals");
      }
    } else {
      throw new UnsupportedOperationException(String.format("Unknown field \"%s\"", fieldName));
    }
  }
}
