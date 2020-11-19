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
package com.dremio.nessie.versioned.store.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.dremio.nessie.versioned.impl.L2;

/**
 * This class is responsible for the encoding and decoding of {@link com.dremio.nessie.versioned.impl.L2} to a BSON object.
 */
public class L2Codec implements Codec<L2> {
  /**
   * This deserializes a BSON stream to create an L2 object.
   * @param bsonReader that provides the BSON
   * @param decoderContext not used
   * @return the object created from the BSON stream.
   */
  @Override
  public L2 decode(BsonReader bsonReader, DecoderContext decoderContext) {
    bsonReader.readStartDocument();
    //TODO complete this method.
    bsonReader.readEndDocument();

    return null;
  }

  /**
   * This serializes an L2 object into a BSON stream. The serialization is delegated to
   * {@Link com.dremio.nessie.versioned.store.mongodb.codecs.L2ToBsonConverter}
   * @param bsonWriter that encodes each attribute to BSON
   * @param l2 the object to encode
   * @param encoderContext not used
   */
  @Override
  public void encode(BsonWriter bsonWriter, L2 l2, EncoderContext encoderContext) {
    bsonWriter.writeStartDocument();
    L2ToBsonConverter l2ToBsonConverter = new L2ToBsonConverter(bsonWriter);
    bsonWriter.writeEndDocument();
  }

  /**
   * A getter of the class being encoded.
   * @return the class being encoded
   */
  @Override
  public Class<L2> getEncoderClass() {
    return L2.class;
  }
}