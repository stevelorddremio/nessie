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
package org.projectnessie.versioned.dynamodb;

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeId;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.idValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.idsList;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.string;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

abstract class DynamoBaseValue<C extends BaseValue<C>> implements BaseValue<C> {

  static final String DT = "dt";
  static final String ID = Store.KEY_NAME;

  final Map<String, AttributeValue> entity = new HashMap<>();

  DynamoBaseValue(ValueType<C> valueType) {
    entity.put(ValueType.SCHEMA_TYPE, string(valueType.getValueName()));
  }

  @Override
  public C id(Id id) {
    return addEntitySafe(ID, idValue(id));
  }

  /**
   * Adds an {@link AttributeValue} that consists of a list of {@link Id}s.
   */
  C addIdList(String key, Stream<Id> ids) {
    return addEntitySafe(key, idsList(ids));
  }

  /**
   * Adds the {@link AttributeValue} for the given key for the final entity.
   */
  @SuppressWarnings("unchecked")
  C addEntitySafe(String key, AttributeValue value) {
    AttributeValue old = entity.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'entity' map. Old={" + old + "} current={" + value + "}");
    }
    return (C) this;
  }

  @Override
  public C dt(long dt) {
    return addEntitySafe(DT, AttributeValueUtil.number(dt));
  }

  Map<String, AttributeValue> build() {
    checkPresent(ID, "id");
    return entity;
  }

  void checkPresent(String id, String name) {
    Preconditions.checkArgument(entity.containsKey(id),
        String.format("Method %s of consumer %s has not been called", name, getClass().getSimpleName()));
  }

  void checkNotPresent(String id, String name) {
    Preconditions.checkArgument(!entity.containsKey(id),
        String.format("Method %s of consumer %s must not be called", name, getClass().getSimpleName()));
  }

  static <C extends BaseValue<C>> C baseToConsumer(Map<String, AttributeValue> entity, C consumer) {
    return consumer.id(deserializeId(entity, ID))
        .dt(AttributeValueUtil.getDt(entity));
  }

}
