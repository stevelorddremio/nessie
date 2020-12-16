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

import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.MongoDBConditionExpressionAliasVisitor;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.tests.AbstractTestStore;

/**
 * A test class that contains MongoDB specific tests.
 */
@ExtendWith(LocalMongo.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMongoDBStore extends AbstractTestStore<MongoDBStore> {
  private static final String testDatabaseName = "mydb";
  private String connectionString;
  private static final MongoDBAliasCollectorImpl COLLECTOR = new MongoDBAliasCollectorImpl();
  private static final ConditionExpressionAliasVisitor CONDITION_EXPRESSION_ALIAS_VISITOR = new MongoDBConditionExpressionAliasVisitor();

  @BeforeAll
  void init(String connectionString) {
    this.connectionString = connectionString;
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected MongoDBStore createStore() {
    final MongoStoreConfig config = new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };

    return new MongoDBStore(config);
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }

  /**
   * This test creates a branch which has two commits in the history.
   * The test is to try to delete the branch specifying the commit history size should be one.
   * The delete should fail.
   */
  @Test
  public void deleteSelectedByConditionExpression1() {
    final ExpressionPath commits = ExpressionPath.builder("commits").build();
    final Entity ONE = Entity.ofNumber(1);

    ConditionExpression conditionExpression1 = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(commits), ONE))
      .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, false, Optional.of(conditionExpression1));
  }

  /**
   * This test creates a branch which has two commits in the history.
   * The test is to try to delete the branch specifying the commit history size should be two.
   * The delete should succeed.
   */
  @Test
  public void deleteSelectedByConditionExpression2() {
    final ExpressionPath commits = ExpressionPath.builder("commits").build();
    final Entity TWO = Entity.ofNumber(2);

    ConditionExpression conditionExpression2 = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(commits), TWO))
      .accept(CONDITION_EXPRESSION_ALIAS_VISITOR, COLLECTOR);
    deleteConditional(SampleEntities.createBranch(random), ValueType.REF, true, Optional.of(conditionExpression2));
  }
}
