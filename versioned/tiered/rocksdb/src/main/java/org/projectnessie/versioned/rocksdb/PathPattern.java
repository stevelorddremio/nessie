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

import java.util.ArrayList;
import java.util.List;

import org.projectnessie.versioned.impl.condition.ExpressionPath;

/**
 * Path patterns provide a simple way to verify that an expression path matches what is expected. All
 * of the path will be matched against the pattern. An expression path consists of a sequence of name
 * and position components. For example, you could have the path "foo[0]". This path has the following components:
 * <ol>
 *   <li>foo - a name</li>
 *   <li>[0] - an index</li>
 *   <li>bar - a name</li>
 *   <li>[1] - an index</li>
 * </ol>
 * Path patterns support exact matching on parts of the path, and any matching. The following patterns all
 * match the example path above.
 * <ul>
 *   <li>new PathPattern().anyName().anyPosition()</li>
 *   <li>new PathPattern().anyName().positionEquals(0)</li>
 *   <li>new PathPattern().nameEquals("foo").anyPosition()</li>
 *   <li>new PathPattern().nameEquals("foo").positionEquals(0)</li>
 * </ul>
 * After building up a path pattern, you can test a path against it with the match method. ex.
 * <pre>
 *   boolean matches = new PathPattern().nameEquals("foo").anyPosition().nameEquals("bar").anyPosition().matches(path);
 * </pre>
 */
class PathPattern {
  private final List<java.util.function.Function<ExpressionPath.PathSegment, Boolean>> pathPatternElements = new ArrayList<>();

  /**
   * Adds an exact name match path component.
   * @param name the name to match against
   * @return the object the pattern component is added to
   */
  PathPattern nameEquals(String name) {
    pathPatternElements.add((p) -> p.isName() && name.equals(p.asName().getName()));
    return this;
  }

  /**
   * Adds an any name match path component.
   * @return the object the pattern component is added to
   */
  PathPattern anyName() {
    pathPatternElements.add(ExpressionPath.PathSegment::isName);
    return this;
  }

  /**
   * Adds an exact position match component.
   * @param position the position to match against
   * @return the object the pattern component is added to
   */
  PathPattern positionEquals(int position) {
    pathPatternElements.add((p) -> p.isPosition() && position == p.asPosition().getPosition());
    return this;
  }

  /**
   * Adds an any position match path component.
   * @return the object the pattern component is added to
   */
  PathPattern anyPosition() {
    pathPatternElements.add(ExpressionPath.PathSegment::isPosition);
    return this;
  }

  /**
   * Tests a path to see if it matches the configured pattern.
   * @param path the path to test
   * @return true if the path matches
   */
  boolean matches(ExpressionPath.PathSegment path) {
    for (java.util.function.Function<ExpressionPath.PathSegment, Boolean> pathPatternElement : pathPatternElements) {
      if (path == null || !pathPatternElement.apply(path)) {
        return false;
      }

      path = path.getChild().orElse(null);
    }

    return path == null;
  }
}
