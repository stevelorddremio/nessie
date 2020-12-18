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

import java.util.stream.Collectors;

import org.bson.BsonBinary;

import com.dremio.nessie.versioned.impl.condition.AliasCollector;
import com.dremio.nessie.versioned.store.Entity;

/**
 * A MongoDB specific implementation of @{com.dremio.nessie.versioned.impl.condition.AliasCollector}.
 */
public class MongoDBAliasCollectorImpl implements AliasCollector {

  /**
   * Converts any special characters or reserved words that cannot be used in queries of MongoDB.
   * Currently there are no restrictions on what is stored.
   * @param unescaped the string to check for characters or reserved words.
   * @return the converted string.
   */
  @Override
  public String escape(String unescaped) {
    return unescaped;
  }

  /**
   * Converts objects into their String equivalent for use in MongoDB queries.
   * @param value the object to convert.
   * @return the string equivalent of value.
   */
  @Override
  public String alias(Entity value) {
    switch (value.getType()) {
      case MAP:
        return "{ $map {" + value.getMap().entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), alias(e.getValue())))
          .collect(Collectors.joining(", ")) + "}}";
      case LIST:
        return "[" + value.getList().stream().map(Entity::toString).collect(Collectors.joining(", ")) + "]";
      case NUMBER:
        return String.valueOf(value.getNumber());
      case STRING:
        return value.getString();
      case BINARY:
        return new BsonBinary(value.getBinary().toByteArray()).toString();
      case BOOLEAN:
        return Boolean.toString(value.getBoolean());
      default:
        throw new UnsupportedOperationException("Unable to convert: " + value.getType());
    }
  }
}
