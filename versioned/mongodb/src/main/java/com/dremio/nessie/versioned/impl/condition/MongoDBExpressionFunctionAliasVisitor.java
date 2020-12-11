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
  public ExpressionFunction aliasVisit(ExpressionFunction expressionFunction, List<Value> arguments, ExpressionFunction.FunctionName name, AliasCollector c) {
    // TODO check v.getPath().acceptAlias() is correct.
    //  We might be able to provide acceptAlias as part of Value interface which would change the call to v.acceptAlias(...
    return new ExpressionFunction(name, arguments.stream().map(v -> getArgumentValue(v, c)).collect(ImmutableList.toImmutableList()));
  }

  Value getArgumentValue(Value v, AliasCollector c) {
    switch (v.getType()) {
      case PATH:
        ExpressionPathAliasVisitor expressionPathAliasVisitor = new MongoDBExpressionPathAliasVisitor();
        return v.getPath().acceptAlias(expressionPathAliasVisitor, c);
      case VALUE:
        //TODO provide correct Value alias visitor
        throw new UnsupportedOperationException();
      case FUNCTION:
        ExpressionFunctionAliasVisitor expressionFunctionAliasVisitor = new MongoDBExpressionFunctionAliasVisitor();
        return v.getFunction().acceptAlias(expressionFunctionAliasVisitor, c);
      default:
        throw new UnsupportedOperationException();
    }
  }

}
