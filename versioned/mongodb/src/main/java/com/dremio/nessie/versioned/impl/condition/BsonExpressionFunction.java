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

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * This provides a separation of queries on @{ExpressionFunction} from the object itself.
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class BsonExpressionFunction implements ExpressionFunctionVisitor<Bson> {

  /**
   * Creates a BSON representation of the ConditionExpression object.
   * @param expressionFunction the object to convert.
   * @return BSON representation of expressionFunction
   */
  public Bson as(ExpressionFunction expressionFunction) {
    return expressionFunction.accept(this);
  }

  /**
   * This is a callback method that ExpressionFunction will call when this visitor is accepted.
   * Its purpose is c reates a BSON representation of the ExpressionFunction object.
   * @param expressionFunction the object to be converteds
   * @return The ExpressionFunction represented as Bson
   */
  @Override
  public Bson visitAs(ExpressionFunction expressionFunction, List<Value> arguments, ExpressionFunction.FunctionName name) {
    Document doc = new Document();
    if (arguments.size() == name.argCount) {
      switch (name) {
        case EQUALS:
          return doc.append(arguments.get(0).asString(), arguments.get(1).asString());
        case SIZE:
          return doc.append("$size", arguments.get(0).asString());
        default:
          break;
      }
    }
    return doc;
  }

}
