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

package org.projectnessie.versioned.rocksdb;

import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateClauseVisitor;
import org.projectnessie.versioned.store.Entity;

/**
 * This provides a separation of generation of UpdateFunction from @{UpdateExpression} from the object itself.
 */
class RocksDBUpdateClauseVisitor implements UpdateClauseVisitor<UpdateFunction> {
  static final RocksDBUpdateClauseVisitor ROCKS_DB_UPDATE_CLAUSE_VISITOR = new RocksDBUpdateClauseVisitor();

  @Override
  public UpdateFunction visit(RemoveClause clause) {
    return ImmutableRemoveFunction.builder().operator(UpdateFunction.Operator.REMOVE).path(clause.getPath()).build();
  }

  @Override
  public UpdateFunction visit(SetClause clause) {
    switch (clause.getValue().getType()) {
      case VALUE:
        return ImmutableSetFunction.builder()
            .operator(UpdateFunction.Operator.SET)
            .subOperator(UpdateFunction.SetFunction.SubOperator.EQUALS)
            .path(clause.getPath())
            .value(clause.getValue().getValue())
            .build();
      case FUNCTION:
        return ImmutableSetFunction.builder()
            .operator(UpdateFunction.Operator.SET)
            .subOperator(UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST)
            .path(clause.getPath())
            .value(handleFunction(clause.getValue().getFunction()))
            .build();
      default:
        throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
    }
  }

  private Entity handleFunction(ExpressionFunction expressionFunction) {
    if (ExpressionFunction.FunctionName.LIST_APPEND == expressionFunction.getName()) {
      return expressionFunction.getArguments().get(1).getValue();
    }
    throw new UnsupportedOperationException(String.format("Unsupported Set function: %s", expressionFunction.getName()));
  }
}
