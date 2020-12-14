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

import com.google.common.collect.ImmutableList;

/*
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class MongoDBExpressionFunctionAliasVisitor implements ExpressionFunctionAliasVisitor {
  @Override
  public ExpressionFunction visit(ExpressionFunction expressionFunction, List<Value> arguments,
                                  ExpressionFunction.FunctionName name, AliasCollector collector) {
    return new ExpressionFunction(name, arguments.stream().map(v -> getArgumentValue(v, collector))
      .collect(ImmutableList.toImmutableList()));
  }

  /**
   * Returns an aliased sub class equivalent to the type of Value.
   * @param value
   * @param collector
   * @return
   */
  Value getArgumentValue(Value value, AliasCollector collector) {
    switch (value.getType()) {
      case PATH:
        final ExpressionPathAliasVisitor expressionPathAliasVisitor = new MongoDBExpressionPathAliasVisitor();
        return value.getPath().accept(expressionPathAliasVisitor, collector);
      case VALUE:
        final ValueAliasVisitor valueAliasVisitor = new MongoDBValueAliasVisitor();
        return value.accept(valueAliasVisitor, collector);
      case FUNCTION:
        final ExpressionFunctionAliasVisitor expressionFunctionAliasVisitor = new MongoDBExpressionFunctionAliasVisitor();
        return value.getFunction().accept(expressionFunctionAliasVisitor, collector);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
