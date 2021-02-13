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

import java.util.Objects;

import org.immutables.value.Value.Immutable;

/**
 * An update that is peformed on an entity.
 */
@Immutable
abstract class UpdateFunction extends Function {
  /**
   * An enum encapsulating.
   */
  enum Operator {
    // An operator to remove some part or all of an entity.
    REMOVE,

    // An operator to set some part or all of an entity.
    SET
  }

  /**
   * Compares for equality with a provided UpdateFunction object.
   * @param object  the object to compare
   * @return true if this is equal to provided object
   */
  @Override
  public boolean equals(Object object) {
    if (object == this) {
      return true;
    }

    if (!(object instanceof UpdateFunction)) {
      return false;
    }

    final UpdateFunction function = (UpdateFunction) object;
    return (getOperator().equals(function.getOperator())
      && getPath().equals(function.getPath())
      && getValue().equals(function.getValue()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOperator(), getPath(), getValue());
  }

  abstract Operator getOperator();

  /**
   * Builds an immutable representation of this class.
   * @return the builder
   */
  public static ImmutableUpdateFunction.Builder builder() {
    return ImmutableUpdateFunction.builder();
  }
}
