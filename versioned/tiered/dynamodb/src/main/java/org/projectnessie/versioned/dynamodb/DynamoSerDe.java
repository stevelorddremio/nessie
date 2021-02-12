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

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.attributeValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeId;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.tiered.CommitMetadata;
import org.projectnessie.versioned.tiered.Fragment;
import org.projectnessie.versioned.tiered.L1;
import org.projectnessie.versioned.tiered.L2;
import org.projectnessie.versioned.tiered.L3;
import org.projectnessie.versioned.tiered.Ref;
import org.projectnessie.versioned.tiered.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

final class DynamoSerDe {

  private static final Map<ValueType<?>, Supplier<DynamoBaseValue<?>>> ENTITY_MAP_PRODUCERS =
      ImmutableMap.<ValueType<?>, Supplier<DynamoBaseValue<?>>>builder()
          .put(ValueType.L1, DynamoL1::new)
          .put(ValueType.L2, DynamoL2::new)
          .put(ValueType.L3, DynamoL3::new)
          .put(ValueType.COMMIT_METADATA, () -> new DynamoWrappedValue<>(ValueType.COMMIT_METADATA))
          .put(ValueType.VALUE, () -> new DynamoWrappedValue<>(ValueType.VALUE))
          .put(ValueType.REF, DynamoRef::new)
          .put(ValueType.KEY_FRAGMENT, DynamoFragment::new)
          .build();
  private static final Map<ValueType<?>, BiConsumer<Map<String, AttributeValue>, BaseValue<?>>> DESERIALIZERS =
      ImmutableMap.<ValueType<?>, BiConsumer<Map<String, AttributeValue>, BaseValue<?>>>builder()
          .put(ValueType.L1, (e, c) -> DynamoL1.toConsumer(e, (L1) c))
          .put(ValueType.L2, (e, c) -> DynamoL2.toConsumer(e, (L2) c))
          .put(ValueType.L3, (e, c) -> DynamoL3.toConsumer(e, (L3) c))
          .put(ValueType.COMMIT_METADATA, (e, c) -> DynamoWrappedValue.produceToConsumer(e, (CommitMetadata) c))
          .put(ValueType.VALUE, (e, c) -> DynamoWrappedValue.produceToConsumer(e, (Value) c))
          .put(ValueType.REF, (e, c) -> DynamoRef.toConsumer(e, (Ref) c))
          .put(ValueType.KEY_FRAGMENT, (e, c) -> DynamoFragment.toConsumer(e, (Fragment) c))
          .build();

  static {
    if (!ENTITY_MAP_PRODUCERS.keySet().equals(DESERIALIZERS.keySet())) {
      throw new UnsupportedOperationException("The enum-maps ENTITY_MAP_PRODUCERS and DESERIALIZERS "
          + "are not equal. This is a bug in the implementation of DynamoSerDe.");
    }
    if (!ENTITY_MAP_PRODUCERS.keySet().containsAll(ValueType.values())) {
      throw new UnsupportedOperationException(String.format("The implementation of the Dynamo backend does not have "
              + "implementations for all supported value-type. Supported by Dynamo: %s, available: %s. "
              + "This is a bug in the implementation of DynamoSerDe.",
          ENTITY_MAP_PRODUCERS.keySet(),
          ValueType.values()));
    }
  }

  private DynamoSerDe() {
    // empty
  }

  /**
   * Serialize using a DynamoDB native consumer to a DynamoDB entity map.
   * <p>
   * The actual entity to serialize is not passed into this method, but this method calls
   * a Java {@link Consumer} that receives an instance of {@link DynamoBaseValue} that receives
   * the entity components.
   * </p>
   */
  @SuppressWarnings("unchecked")
  public static <C extends BaseValue<C>> Map<String, AttributeValue> serializeWithConsumer(
      SaveOp<C> saveOp) {
    Preconditions.checkNotNull(saveOp, "saveOp parameter is null");

    // No need for any 'type' validation - that's done in the static initializer
    C consumer = (C) ENTITY_MAP_PRODUCERS.get(saveOp.getType()).get();

    saveOp.serialize(consumer);

    return ((DynamoBaseValue<C>) consumer).build();
  }

  /**
   * Deserialize the given {@code entity} as the given {@link ValueType type} directly into
   * the given {@code consumer}.
   *
   * @param valueType type of the entity to deserialize
   * @param entity entity to deserialize
   * @param consumer consumer that receives the deserialized parts of the entity
   * @param <C> type of the consumer
   */
  public static <C extends BaseValue<C>> void deserializeToConsumer(
      ValueType<C> valueType, Map<String, AttributeValue> entity, BaseValue<C> consumer) {
    Preconditions.checkNotNull(valueType, "valueType parameter is null");
    Preconditions.checkNotNull(entity, "entity parameter is null");
    Preconditions.checkNotNull(consumer, "consumer parameter is null");
    String loadedType = Preconditions.checkNotNull(attributeValue(entity, ValueType.SCHEMA_TYPE).s(),
        "Schema-type ('t') in entity is not a string");
    Id id = deserializeId(entity, Store.KEY_NAME);
    Preconditions.checkNotNull(loadedType,
        "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(valueType.getValueName().equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.",
        id.getHash(), valueType.name(), loadedType);

    // No need for any 'valueType' validation against the static map - that's done in the static initializer
    DESERIALIZERS.get(valueType).accept(entity, consumer);
  }
}
