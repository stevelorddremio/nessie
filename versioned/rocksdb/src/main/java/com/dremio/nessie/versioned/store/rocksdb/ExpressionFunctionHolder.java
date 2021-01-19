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

import com.dremio.nessie.versioned.store.Entity;

public class ExpressionFunctionHolder {
  static final String EQUALS = "equals";
  static final String SIZE = "size";

  String operator;
  String path;
  Entity value;

  public ExpressionFunctionHolder(String operator, String path, Entity value) {
    this.operator = operator;
    this.path = path;
    this.value = value;
  }
}
