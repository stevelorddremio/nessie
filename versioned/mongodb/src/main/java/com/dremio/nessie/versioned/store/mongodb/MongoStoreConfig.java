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

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class MongoStoreConfig {
  public abstract String getConnectionString();

  @Default
  public String getDatabaseName() {
    return "nessie";
  }

  @Default
  public String getRefTableName() {
    return "refs";
  }

  @Default
  public String getL1TableName() {
    return "l1";
  }

  @Default
  public String getL2TableName() {
    return "l2";
  }

  @Default
  public String getL3TableName() {
    return "l3";
  }

  @Default
  public String getValueTableName() {
    return "values";
  }

  @Default
  public String getKeyListTableName() {
    return "keys";
  }

  @Default
  public String getMetadataTableName() {
    return "commit_metadata";
  }

  @Default
  public long getTimeoutMs() {
    return 5000;
  }

  public static ImmutableMongoStoreConfig.Builder builder() {
    return ImmutableMongoStoreConfig.builder();
  }
}
