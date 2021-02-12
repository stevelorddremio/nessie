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
package org.projectnessie.versioned.tiered;

import org.projectnessie.versioned.store.Id;

/**
 * Base interface for all consumers.
 * <p>
 * Do not implement this interface in non-abstract implementation classes!
 * </p>
 * <p>
 * Implementations must return a shared state ({@code this}) from its method.
 * </p>
 */
public interface BaseValue<T extends BaseValue<T>> {
  /**
   * The id for this consumer.
   * <p>Must be called exactly once.</p>
   *
   * @param id The id.
   * @return This consumer.
   */
  @SuppressWarnings("unchecked")
  default T id(Id id) {
    return (T) this;
  }

  /**
   * Set the date-time in microseconds since epoch when this object was last modified (inserted/updated).
   * @param dt The date+time in microseconds since epoch.
   * @return This consumer.
   */
  @SuppressWarnings("unchecked")
  default T dt(long dt) {
    return (T) this;
  }
}
