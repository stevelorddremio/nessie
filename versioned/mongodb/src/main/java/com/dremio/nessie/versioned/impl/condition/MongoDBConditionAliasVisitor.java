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

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableList;

/*
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class MongoDBConditionAliasVisitor implements ConditionAliasVisitor {
  /**
   * The callback method in the visitor design pattern.
   * @param conditionExpression The object to be aliased.
   * @param collector the class that does the aliasing.
   * @return the aliased ConditionExpression.
   */
  @Override
  public ConditionExpression visit(ConditionExpression conditionExpression, AliasCollector collector) {
    return ImmutableConditionExpression.builder()
      .functions(conditionExpression.getFunctions().stream()
        .map(f -> f.acceptExpressionFunction(this, collector)).collect(ImmutableList.toImmutableList()))
      .build();
  }

  /**
   * The callback method in the visitor design pattern.
   * @param expressionFunction The object to be aliased.
   * @param arguments constituent arguments of the object to be aliased.
   * @param name the name of the object to be aliased.
   * @param collector the class that does the aliasing.
   * @return the aliased ExpressionFunction.
   */
  @Override
  public ExpressionFunction visit(ExpressionFunction expressionFunction, List<Value> arguments,
                                  ExpressionFunction.FunctionName name, AliasCollector collector) {
    return new ExpressionFunction(name, arguments.stream().map(v -> getArgumentValue(v, collector))
      .collect(ImmutableList.toImmutableList()));
  }

  /**
   * The callback method in the visitor design pattern.
   * @param expressionpath The object to be aliased.
   * @param collector the class that does the aliasing.
   * @return The aliased ExpressionPath.
   */
  @Override
  public ExpressionPath visit(ExpressionPath expressionpath, AliasCollector collector) {
    return ImmutableExpressionPath.builder()
      .root((ExpressionPath.NameSegment) expressionpath.getRoot().accept(this, collector))
      .build();
  }

  /**
   * The callback method in the visitor design pattern.
   * @param nameSegment The object to be aliased.
   * @param collector the class that does the aliasing.
   * @return The aliased ExpressionPath.
   */
  @Override
  public ExpressionPath.NameSegment visit(ExpressionPath.NameSegment nameSegment, AliasCollector collector) {
    return ImmutableNameSegment.builder()
      .name(collector.escape(nameSegment.getName()))
      .child(nameSegment.getChild().map(p -> p.alias(collector)))
      .build();
  }

  /**
   * The callback method in the visitor design pattern.
   * @param value The object to be aliased.
   * @param entity ValueOfEntity value
   * @param collector the class that does the aliasing.
   * @return The aliased Value
   */
  @Override
  public Value visit(Value value, Entity entity, AliasCollector collector) {
    return ExpressionPath.builder(collector.alias(entity)).build();
  }

  /**
   * Returns an aliased sub class equivalent to the type of Value.
   * @param value the value to alias.
   * @param collector the class that does the aliasing.
   * @return The aliased ExpressionPath.
   */
  Value getArgumentValue(Value value, AliasCollector collector) {
    switch (value.getType()) {
      case PATH:
        return value.getPath().acceptExpressionPath(this, collector);
      case VALUE:
        return value.acceptValue(this, collector);
      case FUNCTION:
        return value.getFunction().acceptExpressionFunction(this, collector);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
