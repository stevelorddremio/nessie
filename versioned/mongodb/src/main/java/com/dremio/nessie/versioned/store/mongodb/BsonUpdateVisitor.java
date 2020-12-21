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
package com.dremio.nessie.versioned.store.mongodb;

import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;

import com.dremio.nessie.versioned.impl.condition.AddClause;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateVisitor;

/**
 * This provides a separation of generation of expressions from @{UpdateClause} from the object itself.
 */
class BsonUpdateVisitor implements UpdateVisitor<Bson> {
  @Override
  public Bson visit(AddClause clause) {
    final BsonPathVisitor visitor = new BsonPathVisitor();
    clause.getPath().getRoot().accept(visitor, true);
    if (isArrayPath(clause.getPath())) {
      // TODO: Is it necessary to do this differently from a non-array addition?
      return Updates.push(visitor.toString(), clause.getValue());
    }

    return Updates.set(visitor.toString(), clause.getValue());
  }

  @Override
  public Bson visit(RemoveClause clause) {
    final BsonPathVisitor visitor = new BsonPathVisitor();
    clause.getPath().getRoot().accept(visitor, true);
    return Updates.unset(visitor.toString());
  }

  @Override
  public Bson visit(SetClause clause) {
    return null;
  }

  private boolean isArrayPath(ExpressionPath path) {
    ExpressionPath.PathSegment segment = path.getRoot();
    while (segment.getChild().isPresent()) {
      segment = segment.getChild().get();
    }

    return segment.isPosition();
  }
}
