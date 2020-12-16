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

/*
 * This uses the Visitor design pattern to retrieve object attributes.
 */
public class MongoDBExpressionPathAliasVisitor implements ExpressionPathAliasVisitor {
  /**
   * The callback method in the visitor design pattern.
   * @param expressionpath The object to be aliased.
   * @param collector the class that does the aliasing.
   * @return The aliased ExpressionPath.
   */
  @Override
  public ExpressionPath visit(ExpressionPath expressionpath, AliasCollector collector) {
    final ExpressionPathNameSegmentAliasVisitor expressionPathNameSegmentAliasVisitor = new MongoDBExpressionPathNameSegmentAliasVisitor();
    return ImmutableExpressionPath.builder()
      .root((ExpressionPath.NameSegment) expressionpath.getRoot().accept(expressionPathNameSegmentAliasVisitor, collector))
      .build();
  }
}
