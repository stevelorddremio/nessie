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
package org.projectnessie.versioned.mongodb;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.tiered.BaseWrappedValue;
import org.projectnessie.versioned.tiered.Fragment;
import org.projectnessie.versioned.tiered.L1;
import org.projectnessie.versioned.tiered.L2;
import org.projectnessie.versioned.tiered.L3;
import org.projectnessie.versioned.tiered.Mutation;
import org.projectnessie.versioned.tiered.Ref;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

@SuppressWarnings({"unchecked", "rawtypes"})
final class MongoSerDe {
  static final char ZERO_BYTE = '\u0000';
  private static final Map<ValueType<?>, Function<BsonWriter, MongoBaseValue>> CONSUMERS =
      ImmutableMap.<ValueType<?>, Function<BsonWriter, MongoBaseValue>>builder()
          .put(ValueType.L1, MongoL1::new)
          .put(ValueType.L2, MongoL2::new)
          .put(ValueType.L3, MongoL3::new)
          .put(ValueType.KEY_FRAGMENT, MongoFragment::new)
          .put(ValueType.REF, MongoRef::new)
          .put(ValueType.VALUE, MongoWrappedValue::new)
          .put(ValueType.COMMIT_METADATA, MongoWrappedValue::new)
          .build();
  private static final Map<ValueType<?>, BiConsumer<Document, BaseValue>> DESERIALIZERS =
      ImmutableMap.<ValueType<?>, BiConsumer<Document, BaseValue>>builder()
          .put(ValueType.L1, (d, c) -> MongoL1.produce(d, (L1) c))
          .put(ValueType.L2, (d, c) -> MongoL2.produce(d, (L2) c))
          .put(ValueType.L3, (d, c) -> MongoL3.produce(d, (L3) c))
          .put(ValueType.KEY_FRAGMENT, (d, c) -> MongoFragment.produce(d, (Fragment) c))
          .put(ValueType.REF, (d, c) -> MongoRef.produce(d, (Ref) c))
          .put(ValueType.VALUE, (d, c) -> MongoWrappedValue.produce(d, (BaseWrappedValue) c))
          .put(ValueType.COMMIT_METADATA, (d, c) -> MongoWrappedValue.produce(d, (BaseWrappedValue) c))
          .build();

  private static final String KEY_ADDITION = "a";
  private static final String KEY_REMOVAL = "d";

  static {
    if (!CONSUMERS.keySet().equals(DESERIALIZERS.keySet())) {
      throw new UnsupportedOperationException("The enum-maps ENTITY_MAP_PRODUCERS and DESERIALIZERS "
          + "are not equal. This is a bug in the implementation of MongoSerDe.");
    }
    if (!DESERIALIZERS.keySet().equals(new HashSet<>(ValueType.values()))) {
      throw new UnsupportedOperationException(String.format("The enum-map producerMaps does not "
              + "have producer-maps matching the available value types (%s vs %s).",
          DESERIALIZERS.keySet(), new HashSet<>(ValueType.values())));
    }
  }

  private MongoSerDe() {
    // empty
  }

  static <C extends BaseValue<C>> void produceToConsumer(Document d, ValueType<C> valueType, C consumer) {
    DESERIALIZERS.get(valueType).accept(d, consumer);
  }

  private static <C extends BaseValue<C>> MongoBaseValue<C> newMongoConsumer(ValueType<C> valueType, BsonWriter bsonWriter) {
    return CONSUMERS.get(valueType).apply(bsonWriter);
  }

  static <C extends BaseValue<C>> Bson bsonForValueType(SaveOp<C> saveOp, String updateOperator) {
    return new Bson() {
      @Override
      public <T> BsonDocument toBsonDocument(Class<T> clazz, CodecRegistry codecRegistry) {
        final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName(updateOperator);
        serializeEntity(writer, saveOp);
        writer.writeEndDocument();

        return writer.getDocument();
      }
    };
  }

  static <C extends BaseValue<C>> void serializeEntity(BsonWriter writer, SaveOp<C> saveOp) {
    writer.writeStartDocument();
    MongoBaseValue<C> consumer = newMongoConsumer(saveOp.getType(), writer);
    consumer.id(saveOp.getId());
    saveOp.serialize((C) consumer);
    consumer.build();
    writer.writeEndDocument();
  }

  static BsonBinary serializeId(Id id) {
    return new BsonBinary(id.toBytes());
  }

  static void serializeId(BsonWriter writer, String property, Id id) {
    writer.writeBinaryData(property, new BsonBinary(id.toBytes()));
  }

  static Id deserializeId(Document d, String param) {
    return Id.of(((Binary) d.get(param)).getData());
  }

  static Stream<Id> deserializeIds(Document d, String param) {
    List<Binary> ids = (List<Binary>) d.get(param);
    return ids.stream()
        .map(b -> Id.of(b.getData()));
  }

  static BsonBinary serializeBytes(ByteString value) {
    return new BsonBinary(value.toByteArray());
  }

  static Stream<WithPayload<Key>> deserializeKeys(Document document, String param) {
    List<Object> l = (List<Object>) document.get(param);
    return l.stream()
        .map(o -> (List<String>) o)
        .map(MongoSerDe::deserializeKeyWithPayload);
  }

  static WithPayload<Key> deserializeKeyWithPayload(List<String> keyList) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    String value = keyList.get(0);
    Byte payload = value.length() == 1 && value.charAt(0) == ZERO_BYTE ? null : Byte.parseByte(value);
    keyList.subList(1, keyList.size()).forEach(keyBuilder::addElements);
    return WithPayload.of(payload, keyBuilder.build());
  }

  static void serializeKeyWithPayload(BsonWriter bsonWriter, String prop, WithPayload<Key> key) {
    String payload = key.getPayload() == null ? Character.toString(ZERO_BYTE) : key.getPayload().toString();
    bsonWriter.writeStartArray(prop);
    bsonWriter.writeString(payload);
    key.getValue().getElements().forEach(bsonWriter::writeString);
    bsonWriter.writeEndArray();
  }

  static <X> void serializeArray(BsonWriter writer, String prop, Stream<X> src, BiConsumer<BsonWriter, X> inner) {
    writer.writeStartArray(prop);
    src.forEach(e -> inner.accept(writer, e));
    writer.writeEndArray();
  }

  static void serializeKeyMutation(BsonWriter writer, Mutation keyMutation) {
    WithPayload<Key> key;
    switch (keyMutation.getType()) {
      case ADDITION:
        key = WithPayload.of(((Mutation.Addition) keyMutation).getPayload(), keyMutation.getKey());
        break;
      case REMOVAL:
        key = WithPayload.of(null, keyMutation.getKey());
        break;
      default:
        throw new IllegalArgumentException("unknown mutation type " + keyMutation.getType());
    }
    writer.writeStartDocument();
    serializeKeyWithPayload(writer, mutationName(keyMutation.getType()), key);
    writer.writeEndDocument();
  }

  private static String mutationName(Mutation.MutationType type) {
    switch (type) {
      case ADDITION:
        return KEY_ADDITION;
      case REMOVAL:
        return KEY_REMOVAL;
      default:
        throw new IllegalArgumentException("unknown mutation type " + type);
    }
  }

  static Stream<Mutation> deserializeKeyMutations(Document d, String param) {
    List<Document> keyMutations = (List<Document>) d.get(param);
    return keyMutations.stream().map(MongoSerDe::deserializeKeyMutation);
  }

  private static Mutation deserializeKeyMutation(Document d) {
    Entry<String, Object> e = d.entrySet().stream().findFirst().get();
    String addRemove = e.getKey();
    WithPayload<Key> key = deserializeKeyWithPayload((List<String>) e.getValue());
    switch (addRemove) {
      case KEY_ADDITION:
        return Mutation.Addition.of(key.getValue(), key.getPayload());
      case KEY_REMOVAL:
        return Mutation.Removal.of(key.getValue());
      default:
        throw new IllegalArgumentException(String.format("Unsupported key '%s' in key-mutation map", addRemove));
    }
  }
}
