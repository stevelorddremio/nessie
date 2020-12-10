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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.Arrays;
import java.util.Map;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonSerializationException;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Provider for codecs that encode/decode Nessie Values and ID.
 */
class CodecProvider implements org.bson.codecs.configuration.CodecProvider {
  @VisibleForTesting
  static class EntityToBsonConverter {
    /**
     * Write the specified Entity attributes to BSON.
     * @param writer the BSON writer to write to.
     * @param attributes the Entity attributes to serialize.
     */
    @VisibleForTesting
    void write(BsonWriter writer, Map<String, Entity> attributes) {
      writer.writeStartDocument();
      attributes.forEach((k, v) -> writeField(writer, k, v));
      writer.writeEndDocument();
    }

    /**
     * This creates a single field in BSON format that represents entity.
     * @param writer the BSON writer to write to.
     * @param field the name of the field as represented in BSON
     * @param value the entity that will be serialized.
     */
    private void writeField(BsonWriter writer, String field, Entity value) {
      writer.writeName(field);
      writeSingleValue(writer, value);
    }

    /**
     * Writes a single Entity to BSON.
     * Invokes different write methods based on the underlying {@code com.dremio.nessie.versioned.store.Entity} type.
     *
     * @param writer the BSON writer to write to.
     * @param value the value to convert to BSON.
     */
    private void writeSingleValue(BsonWriter writer, Entity value) {
      switch (value.getType()) {
        case MAP:
          writer.writeStartDocument();
          value.getMap().forEach((k, v) -> writeField(writer, k, v));
          writer.writeEndDocument();
          break;
        case LIST:
          writer.writeStartArray();
          value.getList().forEach(v -> writeSingleValue(writer, v));
          writer.writeEndArray();
          break;
        case NUMBER:
          writer.writeInt64(value.getNumber());
          break;
        case STRING:
          writer.writeString(value.getString());
          break;
        case BINARY:
          writer.writeBinaryData(new BsonBinary(value.getBinary().toByteArray()));
          break;
        case BOOLEAN:
          writer.writeBoolean(value.getBoolean());
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unsupported field type: %s", value.getType().name()));
      }
    }
  }

  @VisibleForTesting
  static class BsonToEntityConverter {
    private static final String MONGO_ID_NAME = "_id";

    /**
     * Deserializes a Entity attributes from the BSON stream.
     *
     * @param reader the reader to deserialize the Entity attributes from.
     * @return the Entity attributes.
     */
    @VisibleForTesting
    Map<String, Entity> read(BsonReader reader) {
      final BsonType type = reader.getCurrentBsonType();
      if (BsonType.DOCUMENT != type) {
        throw new BsonSerializationException(
          String.format("BSON serialized data must be a document at the root, type is %s",type));
      }

      return readDocument(reader);
    }

    /**
     * Read a BSON document and deserialize to associated Entity types.
     * @param reader the reader to deserialize the Entities from.
     * @return The deserialized collection of entities with their names.
     */
    private Map<String, Entity> readDocument(BsonReader reader) {
      reader.readStartDocument();
      final ImmutableMap.Builder<String, Entity> mapBuilder = ImmutableMap.builder();

      while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
        final String name = reader.readName();
        if (name.equals(MONGO_ID_NAME)) {
          reader.skipValue();
          continue;
        }

        switch (reader.getCurrentBsonType()) {
          case DOCUMENT:
            mapBuilder.put(name, Entity.ofMap(readDocument(reader)));
            break;
          case ARRAY:
            mapBuilder.put(name, readArray(reader));
            break;
          case BOOLEAN:
            mapBuilder.put(name, Entity.ofBoolean(reader.readBoolean()));
            break;
          case STRING:
            mapBuilder.put(name, Entity.ofString(reader.readString()));
            break;
          case INT32:
            // Writing a small value as an Int64 will still be read as an Int32.
            mapBuilder.put(name, Entity.ofNumber(reader.readInt32()));
            break;
          case INT64:
            mapBuilder.put(name, Entity.ofNumber(reader.readInt64()));
            break;
          case BINARY:
            mapBuilder.put(name, Entity.ofBinary(UnsafeByteOperations.unsafeWrap(reader.readBinaryData().getData())));
            break;
          default:
            throw new UnsupportedOperationException(
              String.format("Unsupported BSON type: %s", reader.getCurrentBsonType().name()));
        }
      }

      reader.readEndDocument();
      return mapBuilder.build();
    }

    /**
     * Read a BSON array and deserialize to associated Entity types.
     * @param reader the reader to deserialize the Entities from.
     * @return The deserialized list-type Entity.
     */
    private Entity readArray(BsonReader reader) {
      reader.readStartArray();
      final ImmutableList.Builder<Entity> listBuilder = ImmutableList.builder();

      while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
        switch (reader.getCurrentBsonType()) {
          case DOCUMENT:
            listBuilder.add(Entity.ofMap(readDocument(reader)));
            break;
          case ARRAY:
            listBuilder.add(readArray(reader));
            break;
          case BOOLEAN:
            listBuilder.add(Entity.ofBoolean(reader.readBoolean()));
            break;
          case STRING:
            listBuilder.add(Entity.ofString(reader.readString()));
            break;
          case INT32:
            listBuilder.add(Entity.ofNumber(reader.readInt32()));
            break;
          case INT64:
            listBuilder.add(Entity.ofNumber(reader.readInt64()));
            break;
          case BINARY:
            listBuilder.add(Entity.ofBinary(UnsafeByteOperations.unsafeWrap(reader.readBinaryData().getData())));
            break;
          default:
            throw new UnsupportedOperationException(
              String.format("Unsupported BSON type: %s", reader.getCurrentBsonType().name()));
        }
      }

      reader.readEndArray();
      return Entity.ofList(listBuilder.build());
    }
  }

  /**
   * Codec responsible for the encoding and decoding of Entities to a BSON objects.
   */
  private static class EntityCodec<C> implements Codec<C> {
    private final Class<C> clazz;
    private final SimpleSchema<C> schema;

    /**
     * Constructor.
     * @param clazz the class type to encode/decode.
     * @param schema the schema of the class.
     */
    EntityCodec(Class<C> clazz, SimpleSchema<C> schema) {
      this.clazz = clazz;
      this.schema = schema;
    }

    /**
     * This deserializes a BSON stream to create an L1 object.
     * @param bsonReader that provides the BSON
     * @param decoderContext not used
     * @return the object created from the BSON stream.
     */
    @Override
    public C decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return schema.mapToItem(BSON_TO_ENTITY_CONVERTER.read(bsonReader));
    }

    /**
     * This serializes an object into a BSON stream. The serialization is delegated to
     * {@link EntityToBsonConverter}
     * @param bsonWriter that encodes each attribute to BSON
     * @param obj the object to encode
     * @param encoderContext not used
     */
    @Override
    public void encode(BsonWriter bsonWriter, C obj, EncoderContext encoderContext) {
      ENTITY_TO_BSON_CONVERTER.write(bsonWriter, schema.itemToMap(obj, true));
    }

    /**
     * A getter of the class being encoded.
     * @return the class being encoded
     */
    @Override
    public Class<C> getEncoderClass() {
      return clazz;
    }
  }

  private static class IdCodec implements Codec<Id> {
    @Override
    public Id decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return Id.of(bsonReader.readBinaryData().getData());
    }

    @Override
    public void encode(BsonWriter bsonWriter, Id id, EncoderContext encoderContext) {
      bsonWriter.writeBinaryData(new BsonBinary(id.toBytes()));
    }

    @Override
    public Class<Id> getEncoderClass() {
      return Id.class;
    }
  }

  @VisibleForTesting
  static final EntityToBsonConverter ENTITY_TO_BSON_CONVERTER = new EntityToBsonConverter();
  @VisibleForTesting
  static final BsonToEntityConverter BSON_TO_ENTITY_CONVERTER = new BsonToEntityConverter();

  private static final Map<Class<?>, Codec<?>> CODECS;

  static {
    final ImmutableMap.Builder<Class<?>, Codec<?>> builder = ImmutableMap.builder();
    Arrays.stream(ValueType.values()).forEach(v ->
        builder.put(v.getObjectClass(), new EntityCodec<>(v.getObjectClass(), v.getSchema()))
    );

    // Specific case where the ID is not encoded as a document, but directly as a binary value. Keep within this provider
    // as Mongo appears to rely on the same provider for all related codecs, and splitting this to a separate provider
    // results in the incorrect codec being used.
    builder.put(Id.class, new IdCodec());
    CODECS = builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    final Codec<T> codec = (Codec<T>)CODECS.get(clazz);
    if (null != codec) {
      return codec;
    }

    // In most cases, the codec for a class is directly entered, but also account for when there are subclasses for
    // a registered class and get the CODEC for that.
    for (Map.Entry<Class<?>, Codec<?>> entry : CODECS.entrySet()) {
      if (entry.getKey().isAssignableFrom(clazz)) {
        return (Codec<T>)entry.getValue();
      }
    }

    return null;
  }
}
