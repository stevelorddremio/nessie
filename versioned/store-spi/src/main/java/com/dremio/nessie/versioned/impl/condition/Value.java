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
package com.dremio.nessie.versioned.impl.condition;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.dremio.nessie.versioned.store.Entity;

/**
 * A marker interface that is exposes a value type in DynamoDB's expression language.
 */
public interface Value extends Aliasable<Value> {

  static Value of(Entity value) {
    return new ValueOfEntity(value);
  }

  @Override
  Value alias(AliasCollector c);

  /**
   * Return the string representation of this string, if possible.
   * @return A DynamoDb expression fragment.
   */
  String asString();

  /**
   * Return the value type of this value.
   * @return A value type.
   */
  Type getType();

  default Entity getValue() {
    throw new IllegalArgumentException();
  }

  default ExpressionPath getPath() {
    throw new IllegalArgumentException();
  }

  default ExpressionFunction getFunction() {
    throw new IllegalArgumentException();
  }

  public static enum Type {
    VALUE, PATH, FUNCTION;
  }

  static class ValueOfEntity implements Value {
    private final Entity value;

    public ValueOfEntity(Entity value) {
      this.value = value;
    }

    @Override
    public Value alias(AliasCollector c) {
      return ExpressionPath.builder(c.alias(value)).build();
    }

    @Override
    public String asString() {
      throw new IllegalArgumentException();
    }

    @Override
    public Type getType() {
      return Type.VALUE;
    }

  }
}
