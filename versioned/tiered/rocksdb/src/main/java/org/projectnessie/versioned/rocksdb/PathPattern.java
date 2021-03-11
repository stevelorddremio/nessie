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

import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.ValueVisitor;
import org.projectnessie.versioned.store.Entity;

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
 *   <li>PathPattern.exact().anyName().anyPosition()</li>
 *   <li>PathPattern.exact().anyName().positionEquals(0)</li>
 *   <li>PathPattern.exact().nameEquals("foo").anyPosition()</li>
 *   <li>PathPattern.exact().nameEquals("foo").positionEquals(0)</li>
 * </ul>
 * After building up a path pattern, you can test a path against it with the match method. ex.
 * <pre>
 *   boolean matches = PathPattern.exact().nameEquals("foo").anyPosition().nameEquals("bar").anyPosition().matches(path);
 * </pre>
 */
class PathPattern implements ValueVisitor<Boolean> {
  private final MatchType matchType;
  private final List<java.util.function.Function<ExpressionPath.PathSegment, Boolean>> pathPatternElements = new ArrayList<>();

  enum MatchType {
    EXACT,
    PREFIX
  }

  private PathPattern(MatchType matchType) {
    this.matchType = matchType;
  }

  /**
   * Create a new exact match pattern starting with a provided name.
   * @param name the name the pattern starts with
   * @return the new PathPattern
   */
  static PathPattern exact(String name) {
    return new PathPattern(MatchType.EXACT).nameEquals(name);
  }

  /**
   * Create a new exact match pattern starting with a provided position.
   * @param position the position the pattern starts with
   * @return the new PathPattern
   */
  static PathPattern exact(int position) {
    return new PathPattern(MatchType.EXACT).positionEquals(position);
  }

  /**
   * Create a new prefix match pattern starting with a provided name.
   * @param name the name the pattern starts with
   * @return the new PathPattern
   */
  static PathPattern prefix(String name) {
    return new PathPattern(MatchType.PREFIX).nameEquals(name);
  }

  /**
   * Create a new prefix match pattern starting with a provided position.
   * @param position the position the pattern starts with
   * @return the new PathPattern
   */
  static PathPattern prefix(int position) {
    return new PathPattern(MatchType.PREFIX).positionEquals(position);
  }

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
   * Not supported.
   * @param entity the Entity to visit.
   * @return throws an UnsupportedOperationException
   */
  @Override
  public Boolean visit(Entity entity) {
    throw new UnsupportedOperationException("Entity objects are not supported by PathPattern.");
  }

  /**
   * Not supported.
   * @param value the ExpressionFunction to visit.
   * @return throws an UnsupportedOperationException
   */
  @Override
  public Boolean visit(ExpressionFunction value) {
    throw new UnsupportedOperationException("ExpressionFunction objects are not supported by PathPattern.");
  }

  /**
   * Tests a path to see if it matches the configured pattern.
   * @param path the path to test
   * @return true if the path matches
   */
  @Override
  public Boolean visit(ExpressionPath path) {
    ExpressionPath.PathSegment currentNode = path.getRoot();

    for (java.util.function.Function<ExpressionPath.PathSegment, Boolean> pathPatternElement : pathPatternElements) {
      if (currentNode == null || !pathPatternElement.apply(currentNode)) {
        return false;
      }

      currentNode = currentNode.getChild().orElse(null);
    }

    return matchType == MatchType.PREFIX || currentNode == null;
  }

  ExpressionPath removePrefix(ExpressionPath path) {
    ExpressionPath.PathSegment currentNode = path.getRoot();

    for (java.util.function.Function<ExpressionPath.PathSegment, Boolean> pathPatternElement : pathPatternElements) {
      if (currentNode == null || !pathPatternElement.apply(currentNode)) {
        return path;
      }

      currentNode = currentNode.getChild().orElse(null);
    }

    if (currentNode != null && currentNode.isName()) {
      ExpressionPath.PathSegment.Builder pathBuilder = ExpressionPath.builder(currentNode.asName().getName());
      currentNode = currentNode.getChild().orElse(null);

      while (currentNode != null) {
        if (currentNode.isName()) {
          pathBuilder = pathBuilder.name(currentNode.asName().getName());
        } else {
          pathBuilder = pathBuilder.position(currentNode.asPosition().getPosition());
        }

        currentNode = currentNode.getChild().orElse(null);
      }

      return pathBuilder.build();
    } else {
      return null;
    }
  }
}
