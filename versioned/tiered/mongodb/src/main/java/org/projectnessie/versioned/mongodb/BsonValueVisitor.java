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
package org.projectnessie.versioned.mongodb;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.internal.Base64;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.Value;
import org.projectnessie.versioned.impl.condition.ValueVisitor;
import org.projectnessie.versioned.store.Entity;

/**
 * This provides a separation of queries on @{ExpressionFunction} from the object itself.
 * This uses the Visitor design pattern to retrieve object attributes.
 */
class BsonValueVisitor implements ValueVisitor<String> {
  static final BsonValueVisitor VALUE_VISITOR = new BsonValueVisitor();

  @Override
  public String visit(Entity value) {
    return toMongoExpression(value);
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
        // Special case SIZE, as the object representation is awkward for how it should be represented by MongoDB.
        if (isSize(arguments.get(0))) {
          return String.format("{%s: {\"$size\": %s}}",
            arguments.get(0).getFunction().getArguments().get(0).accept(this), arguments.get(1).accept(this));
        }

        return String.format("{%s: %s}", arguments.get(0).accept(this), arguments.get(1).accept(this));
      default:
        throw new UnsupportedOperationException(String.format("%s is not a supported top-level MongoDB function.", name));
    }
  }

  @Override
  public String visit(ExpressionPath value) {
    return value.getRoot().accept(BsonPathVisitor.INSTANCE, true);
  }

  private boolean isSize(Value value) {
    return (value.getType() == Value.Type.FUNCTION) && (((ExpressionFunction)value).getName() == ExpressionFunction.FunctionName.SIZE);
  }

  /**
   * Convert the Entity chain to an equivalent MongoDB expression.
   * @param value the entity to convert.
   * @return the MongoDB string representation of the Entity.
   */
  static String toMongoExpression(Entity value) {
    switch (value.getType()) {
      case MAP:
        return "{" + value.getMap().entrySet().stream().map(e -> String.format("\"%s\": %s", e.getKey(), toMongoExpression(e.getValue())))
          .collect(Collectors.joining(", ")) + "}";
      case LIST:
        return "[" + value.getList().stream().map(BsonValueVisitor::toMongoExpression).collect(Collectors.joining(", ")) + "]";
      case NUMBER:
        return String.valueOf(value.getNumber());
      case STRING:
        return "\"" + value.getString() + "\"";
      case BINARY:
        return String.format("{\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}", Base64.encode(value.getBinary().toByteArray()));
      case BOOLEAN:
        return Boolean.toString(value.getBoolean());
      default:
        throw new UnsupportedOperationException(String.format("Unable to convert type '%s' to String.", value.getType()));
    }
  }
}
