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
import java.util.stream.Collectors;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

/**
 * This class allows retrieval of ConditionExpression objects in BSON format.
 */
public class BsonConditionExpressionVisitor implements ConditionExpressionVisitor<Bson> {
  // This class provides a separation of queries on @{ConditionExpression} from the object itself.
  // This uses the Visitor design pattern to retrieve object attributes.

  /**
  * This is a callback method that ConditionExpression will call when this visitor is accepted.
  * It creates a BSON representation of the ConditionExpression object.
  * @param conditionExpression to be converted into BSON.
  * @return the converted ConditionExpression object in BSON format.
  */
  @Override
  public Bson visit(final ConditionExpression conditionExpression) {
    final BsonExpressionFunctionVisitor bsonExpressionFunctionVisitor = new BsonExpressionFunctionVisitor();
    final List<Bson> expressionFunctionList = conditionExpression.getFunctions().stream()
      .map(f -> f.accept(bsonExpressionFunctionVisitor))
      .collect(Collectors.toList());

    return (expressionFunctionList.size() == 1) ?
        expressionFunctionList.get(0) :
        Filters.and(expressionFunctionList);
  }
}
