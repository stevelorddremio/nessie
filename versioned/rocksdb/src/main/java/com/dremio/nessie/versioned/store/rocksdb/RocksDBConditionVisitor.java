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

import java.security.InvalidParameterException;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.Value;
import com.dremio.nessie.versioned.impl.condition.ValueVisitor;
import com.dremio.nessie.versioned.store.Entity;

/**
 * This class allows conversion of ConditionExpression objects to ConditionExpressionHolder objects.
 */
class RocksDBConditionVisitor implements ConditionExpressionVisitor<String> {
  /**
   * This provides a separation of queries on @{ExpressionFunction} from the object itself.
   * This uses the Visitor design pattern to retrieve object attributes.
   */
  private static class RocksDBValueVisitor implements ValueVisitor<String> {
    @Override
    public String visit(Value value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String visit(ExpressionFunction value) {
      final ExpressionFunction.FunctionName name = value.getName();
      final List<Value> arguments = value.getArguments();
      if (arguments.size() != name.getArgCount()) {
        throw new InvalidParameterException(
          String.format("Number of arguments provided [%d] does not match the number expected [%d] for %s.",
            arguments.size(), name.getArgCount(), name));
      }

      switch (name) {
        case EQUALS:
          // Special case SIZE, as the object representation is not contained in one level of ExpressionFunction.
          if (isSize(arguments.get(0))) {
            return String.format("%s,%s,%s", ExpressionFunctionHolder.SIZE,
              arguments.get(0).getFunction().getArguments().get(0).accept(this), arguments.get(1).accept(this));
          }

          return String.format("%s,%s,%s", ExpressionFunctionHolder.EQUALS, arguments.get(0).accept(this), arguments.get(1).accept(this));
        default:
          throw new UnsupportedOperationException(String.format("%s is not a supported top-level RocksDB function.", name));
      }
    }

    @Override
    public String visit(ExpressionPath value) {
      return value.getRoot().accept(RocksDBPathVisitor.INSTANCE, true);
    }

    private boolean isSize(Value value) {
      return (value.getType() == Value.Type.FUNCTION) && (((ExpressionFunction)value).getName() == ExpressionFunction.FunctionName.SIZE);
    }
  }

  static final RocksDBValueVisitor VALUE_VISITOR = new RocksDBValueVisitor();

  /**
  * This is a callback method that ConditionExpression will call when this visitor is accepted.
  * It creates a ConditionExpressionHolder representation of the ConditionExpression object.
  * @param conditionExpression to be converted into BSON.
  * @return the converted ConditionExpression object in BSON format.
  */
  @Override
  public String visit(final ConditionExpression conditionExpression) {
    final String functions = conditionExpression.getFunctions().stream()
        .map(f -> f.accept(VALUE_VISITOR))
        .collect(Collectors.joining("& "));

    return functions;
  }
}
