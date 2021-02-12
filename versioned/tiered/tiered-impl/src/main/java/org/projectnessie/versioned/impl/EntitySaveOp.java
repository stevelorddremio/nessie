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
package org.projectnessie.versioned.impl;

import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

final class EntitySaveOp<C extends BaseValue<C>, E extends PersistentBase<C>> extends SaveOp<C> {
  private final E value;

  EntitySaveOp(ValueType<C> type, E value) {
    super(type, value.getId());
    this.value = value;
  }

  @Override
  public void serialize(C consumer) {
    value.applyToConsumer(consumer);
  }
}
