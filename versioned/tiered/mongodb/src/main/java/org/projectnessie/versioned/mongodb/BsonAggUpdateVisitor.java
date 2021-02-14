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

import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateClauseVisitor;

/**
 * This provides a separation of generation of BSON expressions from @{UpdateExpression} from the object itself.
 */
class BsonAggUpdateVisitor implements UpdateClauseVisitor<BsonAggUpdateVisitor.StageOp> {
  static class StageOp {
    final String path;
    final String index;
    final String operation;

    StageOp(String path, String index, String operation) {
      this.path = path;
      this.index = index;
      this.operation = operation;
    }
  }

  static final BsonAggUpdateVisitor CLAUSE_VISITOR = new BsonAggUpdateVisitor();

  @Override
  public StageOp visit(RemoveClause clause) {
    final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);
    if (BsonUpdateVisitor.isArrayPath(clause.getPath())) {
      // Mongo has no way of deleting an element from an array, it by default leaves a NULL element. To remove the
      // element in one operation, splice together the array around the index to remove.
      final int lastIndex = path.lastIndexOf(".");
      final String arrayPath = path.substring(0, lastIndex);
      final String arrayIndex = path.substring(lastIndex + 1);
      return new StageOp(arrayPath, arrayIndex, String.format("{$addFields: {\"%1$s\": {$concatArrays: [{$slice:[ \"$%1$s\", %2$s]}, "
          + "{$slice:[ \"$%1$s\", {$add:[1, %2$s]}, {$size:\"$%1$s\"}]}]}}}",
          arrayPath, arrayIndex));
    }

    return new StageOp(path, "", String.format("{$project: {\"%s\": 0}}", path));
  }

  @Override
  public StageOp visit(SetClause clause) {
    throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
  }
}
