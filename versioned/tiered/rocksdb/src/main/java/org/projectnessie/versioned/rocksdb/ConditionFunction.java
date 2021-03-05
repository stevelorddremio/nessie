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
import org.projectnessie.versioned.store.Entity;

/**
 * A condition that is asserted against an entity.
 */
@Immutable
abstract class ConditionFunction implements Function {
  /**
   * An enum encapsulating.
   */
  enum Operator {
    // An operator comparing the equality of entities.
    EQUALS,

    // An operator comparing the size of entities.
    SIZE
  }

  /**
   * Compares for equality with a provided ConditionFunction object.
   * @param object  the object to compare
   * @return true if this is equal to provided object
   */
  @Override
  public boolean equals(Object object) {
    if (object == this) {
      return true;
    }

    if (!(object instanceof ConditionFunction)) {
      return false;
    }

    final ConditionFunction function = (ConditionFunction) object;
    return (getOperator().equals(function.getOperator())
      && getPath().equals(function.getPath())
      && getValue().equals(function.getValue()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOperator(), getPath(), getValue());
  }

  abstract Operator getOperator();

  abstract Entity getValue();

  /**
   * A utility to aid evaluation of the ConditionFunction in checking for equality on
   * a leaf {@link org.projectnessie.versioned.impl.condition.ExpressionPath.NameSegment}.
   * @return true if both root nameSegment is childless and function has an equality operator
   */
  boolean isRootNameSegmentChildlessAndEquals() {
    return !getRootPathAsNameSegment().getChild().isPresent()
      && getOperator().equals(Operator.EQUALS);
  }
}
