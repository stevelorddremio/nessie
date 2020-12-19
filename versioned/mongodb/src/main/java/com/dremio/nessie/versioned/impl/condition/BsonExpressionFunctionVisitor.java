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

import com.google.common.collect.ArrayListMultimap;
import com.mongodb.client.model.Filters;

/**
 * This provides a separation of queries on @{ExpressionFunction} from the object itself.
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class BsonExpressionFunctionVisitor implements ExpressionFunctionVisitor<Bson> {
  private static final BsonExpressionFunctionVisitor instance = new BsonExpressionFunctionVisitor();
  private static final String OPERAND = "OPERAND";
  private static final String SIZE = "SIZE";

  public static BsonExpressionFunctionVisitor getInstance() {
    return instance;
  }

  private BsonExpressionFunctionVisitor() {
  }

  /**
   * This is a callback method that ExpressionFunction will call when this visitor is accepted.
   * Its purpose is c reates a BSON representation of the ExpressionFunction object.
   * @param expressionFunction the object to be converted
   * @return The ExpressionFunction represented as Bson
   */
  @Override
  public Bson visit(ExpressionFunction expressionFunction) {
    final ExpressionFunction.FunctionName name = expressionFunction.getName();
    final List<Value> arguments = expressionFunction.getArguments();
    if (arguments.size() != name.argCount) {
      throw new InvalidParameterException(
        String.format("Number of arguments provided %d does not match the number expected %d for %s.",
          arguments.size(), name.argCount, name));
    }
    switch (name) {
      case EQUALS:
        final Bson document = createSizeDocument(arguments);
        if (document != null) {
          return document;
        }
        return Filters.eq(arguments.get(0).asString(), arguments.get(1).asString());

      case SIZE:
        throw new UnsupportedOperationException(String.format("%s must be converted as part of an EQUALS ExpressionFunction", name));
      default:
        throw new UnsupportedOperationException(String.format("%s is not a supported function name.", name));
    }
  }

  /**
   * Creates a SIZE representation if the ExpressionFunction is a SIZE type.
   * @param arguments the attributes of the ExpressionFunction.
   * @return the converted class
   */
  public Bson createSizeDocument(List<Value> arguments) {
    ArrayListMultimap<String, String> operands = ArrayListMultimap.create();
    for (Value v : arguments) {
      operands.putAll(addToMap(v));
    }
    if (!operands.containsKey(SIZE)) {
      return null;
    }
    return Filters.size(operands.get(SIZE).get(0), Integer.parseInt(operands.get(OPERAND).get(0)));
  }

  /**
   * Creates a Multimap from the arguments in the value.
   * @param value the item containing arguments.
   * @return the Multimap of the arguments.
   */
  private ArrayListMultimap<String, String> addToMap(Value value) {
    ArrayListMultimap<String, String> map = ArrayListMultimap.create();
    if (value instanceof ExpressionFunction && ((ExpressionFunction)value).getName() == ExpressionFunction.FunctionName.SIZE) {
      map.put(SIZE, ((ExpressionFunction) value).getArguments().get(0).asString());
    } else {
      map.put(OPERAND, value.asString());
    }
    return map;
  }
}
