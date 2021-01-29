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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.dremio.nessie.versioned.store.Entity;

/**
 * An expression that is asserted against an Entity.
 */
public class Function {
  public static final String EQUALS = "equals";
  public static final String SIZE = "size";

  String operator;
  String path;
  Entity value;

  /**
   * ExpressionFunction representation.
   * @param operator type of assertion to be applied
   * @param path the path to be tested
   * @param value the expected value of path
   */
  public Function(String operator, String path, Entity value) {
    this.operator = operator;
    this.path = path;
    this.value = value;
  }

  /**
   * Compares for equality with a provided Function object.
   * @param ex object to compare
   * @return true if this is equal to provided object
   */
  public boolean equals(Function ex) {
    return (this.operator.equals(ex.operator)
        && this.path.equals(ex.path)
        && this.value.equals(ex.value));
  }

  /**
   * Provides path segments as a list. List indexes, if present, are returned with the segment to which they relate.
   * @return the List of path segments.
   */
  public List<String> getPathAsList() {
    return Arrays.asList(path.split(Pattern.quote(".")));
  }

  public String getOperator() {
    return operator;
  }

  public String getPath() {
    return path;
  }

  public Entity getValue() {
    return value;
  }
}
