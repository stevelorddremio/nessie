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
 * Classes requiring visiting rights to alias ExpressionFunction must implement this
 * interface as part of the Visitor design pattern.
 */
public interface ExpressionFunctionAliasVisitor {
  /**
   * The callback method in the visitor design pattern.
   * @param expressionFunction The object to be aliased.
   * @param collector the class that does the aliasing.
   * @return The aliased ExpressionFunction.
   */
  ExpressionFunction visit(ExpressionFunction expressionFunction, List<Value> arguments,
                           ExpressionFunction.FunctionName name, AliasCollector collector);
}