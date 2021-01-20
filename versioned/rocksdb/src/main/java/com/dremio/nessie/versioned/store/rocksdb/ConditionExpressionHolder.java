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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.dremio.nessie.versioned.store.Entity;

public class ConditionExpressionHolder {

  public List<ExpressionFunctionHolder> expressionFunctionHolderList;

  public ConditionExpressionHolder() {
    this.expressionFunctionHolderList = new ArrayList<>();
  }

  /**
   * TODO: This will likely take a ConditionExpression instead. For now we build this object from a string.
   * The format is:
   *    "ExpressionFunction&ExpressionFunction&ExpressionFunction"
   *      where ExpressionFunction format is:
   *      "ExpressionFunctionOperator,ExpressionFunctionPath,ExpressionFunctionValue"
   * @param conditionExpressionStr the string from which to build this object.
   */
  public void build(String conditionExpressionStr) {
    List<String> expressionFunctions = new ArrayList<>(Arrays.asList((conditionExpressionStr.split("\\s*&\\s*"))));
    for (String expressionFunctionStr : expressionFunctions) {
      // Split into three elements
      List<String> elements = new ArrayList<>(Arrays.asList((expressionFunctionStr.split("\\s*,\\s*"))));
      ExpressionFunctionHolder holder = new ExpressionFunctionHolder(elements.get(0), elements.get(1), Entity.ofString(elements.get(2)));
      expressionFunctionHolderList.add(holder);
    }
  }
}
