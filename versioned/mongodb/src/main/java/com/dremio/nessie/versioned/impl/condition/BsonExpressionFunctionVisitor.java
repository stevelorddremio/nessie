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
package com.dremio.nessie.versioned.impl.condition;

import java.security.InvalidParameterException;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

/**
 * This provides a separation of queries on @{ExpressionFunction} from the object itself.
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class BsonExpressionFunctionVisitor implements ExpressionFunctionVisitor<Bson> {

  /**
   * This is a callback method that ExpressionFunction will call when this visitor is accepted.
   * Its purpose is c reates a BSON representation of the ExpressionFunction object.
   * @param expressionFunction the object to be converteds
   * @return The ExpressionFunction represented as Bson
   */
  @Override
  public Bson visit(ExpressionFunction expressionFunction, List<Value> arguments, ExpressionFunction.FunctionName name) {
    if (arguments.size() != name.argCount) {
      throw new InvalidParameterException(
        String.format("Number of arguments provided %d does not match the number expected %d for %s.",
          arguments.size(), name.argCount, name));
    }
    switch (name) {
      case EQUALS:
        final Bson document = expressionFunction.accept(this, arguments.get(0), arguments.get(1));
        if (document != null) {
          return document;
        }
        return Filters.eq(arguments.get(0).asString(), arguments.get(1).asString());

      case SIZE:
        throw new UnsupportedOperationException(String.format("%s must be converted using visitGetSize().", name));
      default:
        throw new UnsupportedOperationException(String.format("%s is not a supported function name.", name));
    }
  }

  /**
   * Creates a Bson representation of a SIZE ConditionExpression from the hierachical representation required by ExpressionFunctions.
   * @param expressionFunction  The object to be represented as class T.
   * @param attributeName the attribute for which size is tested.
   * @param attributeSize the expected size of the attribute.
   * @return the Bson representation of the size object.
   */
  @Override
  public Bson visitGetSize(ExpressionFunction expressionFunction, String attributeName, String attributeSize) {
    return Filters.size(attributeName, Integer.parseInt(attributeSize));
  }
}
