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

class PathPattern {
  private List<java.util.function.Function<ExpressionPath.PathSegment, Boolean>> pathPatternElements = new ArrayList<>();

  public PathPattern nameEquals(String name) {
    pathPatternElements.add((p) -> p != null && p.isName() && name.equals(p.asName().getName()));
    return this;
  }

  public PathPattern anyName() {
    pathPatternElements.add((p) -> p != null && p.isName());
    return this;
  }

  public PathPattern positionEquals(int position) {
    pathPatternElements.add((p) -> p != null && p.isPosition() && position == p.asPosition().getPosition());
    return this;
  }

  public PathPattern anyPosition() {
    pathPatternElements.add((p) -> p != null && p.isPosition());
    return this;
  }

  public boolean matches(ExpressionPath.PathSegment path) {
    for (java.util.function.Function<ExpressionPath.PathSegment, Boolean> pathPatternElement : pathPatternElements) {
      if (!pathPatternElement.apply(path)) {
        return false;
      } else if (path == null) {
        return false;
      }

      path = path.getChild().orElse(null);
    }

    return path == null;
  }
}
