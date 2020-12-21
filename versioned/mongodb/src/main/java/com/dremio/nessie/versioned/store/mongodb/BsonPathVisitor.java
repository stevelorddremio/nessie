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

import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.google.common.base.Preconditions;

/**
 * Visitor for creating Bson valid paths from ExpressionPaths.
 */
class BsonPathVisitor implements ExpressionPath.PathVisitor<Boolean, Void, RuntimeException> {
  private final StringBuilder builder;

  BsonPathVisitor() {
    this(new StringBuilder());
  }

  BsonPathVisitor(StringBuilder builder) {
    this.builder = builder;
  }

  @Override
  public Void visitName(ExpressionPath.NameSegment segment, Boolean first) throws RuntimeException {
    if (!first) {
      builder.append(".");
    }
    builder.append(segment.getName());
    return childOrNull(segment);
  }

  private Void childOrNull(ExpressionPath.PathSegment segment) {
    segment.getChild().ifPresent(c -> c.accept(this,  false));
    return null;
  }

  @Override
  public Void visitPosition(ExpressionPath.PositionSegment segment, Boolean first) throws RuntimeException {
    Preconditions.checkArgument(!first);
    builder.append(".");
    builder.append(segment.getPosition());
    return childOrNull(segment);
  }

  @Override
  public String toString() {
    return "\"" + builder + "\"";
  }
}
