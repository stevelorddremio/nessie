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

/**
 * Classes requiring visiting rights to ConditionExpression must implement this
 * interface as part of the Visitor design pattern.
 * @param <T>
 */
public interface ExpressionFunctionVisitor<T> {
  /**
   * Creates a representation of a ExpressionFunction in the class T.
   * Visitors should call an accept method on ExpressionFunction then this callback method is called.
   */
  T visitAs(ExpressionFunction expressionFunction, List<Value> arguments, ExpressionFunction.FunctionName name);
}