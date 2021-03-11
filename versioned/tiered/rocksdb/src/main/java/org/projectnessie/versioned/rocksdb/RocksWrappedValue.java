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

import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.BaseWrappedValue;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.BaseWrappedValue} providing
 * SerDe and Condition evaluation.
 */
class RocksWrappedValue<C extends BaseWrappedValue<C>> extends RocksBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";
  private final ValueProtos.WrappedValue.Builder builder = ValueProtos.WrappedValue.newBuilder();

  RocksWrappedValue() {
    super();
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case VALUE:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !builder.getValue().equals(function.getValue().getBinary())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  protected void remove(ExpressionPath path) {
    throw new UnsupportedOperationException(String.format("Remove is not supported for \"%s\"", path.asString()));
  }

  @Override
  protected boolean fieldIsList(ExpressionPath path) {
    return false;
  }

  @Override
  protected void appendToList(ExpressionPath path, List<Entity> valuesToAdd) {
    throw new UnsupportedOperationException(String.format("Append to list is not supported for \"%s\"", path.asString()));
  }

  @Override
  protected void set(ExpressionPath path, Entity newValue) {
    if (path.accept(new PathPattern(VALUE))) {
      builder.setValue(newValue.getBinary());
    } else {
      throw new UnsupportedOperationException(String.format("%s is not a valid path for set equals", path.asString()));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public C value(ByteString value) {
    builder.setValue(value);
    return (C) this;
  }

  @Override
  byte[] build() {
    checkPresent(builder.getValue(), VALUE);

    return builder.setBase(buildBase()).build().toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static <C extends BaseWrappedValue<C>> void toConsumer(byte[] value, C consumer) {
    try {
      final ValueProtos.WrappedValue wrappedValue = ValueProtos.WrappedValue.parseFrom(value);
      setBase(consumer, wrappedValue.getBase());
      consumer.value(wrappedValue.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt WrappedValue value encountered when deserializing.", e);
    }
  }

  ByteString getValue() {
    return builder.getValue();
  }
}
