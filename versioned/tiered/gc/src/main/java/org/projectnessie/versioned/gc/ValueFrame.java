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
package org.projectnessie.versioned.gc;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.Value;

import com.google.protobuf.ByteString;

public class ValueFrame {
  private byte[] bytes;
  private long dt;
  private IdFrame id;

  public byte[] getBytes() {
    return bytes;
  }

  public long getDt() {
    return dt;
  }

  public void setDt(long dt) {
    this.dt = dt;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public IdFrame getId() {
    return id;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }

  public ValueFrame() {
  }

  public static Function<Store.Acceptor<Value>, ValueFrame> BUILDER = (a) -> {
    ValueFrame f = new ValueFrame();
    a.applyValue(new Value() {

      @Override
      public Value value(ByteString value) {
        f.bytes = value.toByteArray();
        return this;
      }

      @Override
      public Value id(Id id) {
        f.id = IdFrame.of(id);
        return this;
      }

      @Override
      public Value dt(long dt) {
        f.dt = dt;
        return this;
      }
    });
    return f;
  };

  public static Dataset<ValueFrame> asDataset(Supplier<Store> store, SparkSession spark) {
    return ValueRetriever.dataset(store, ValueType.VALUE, ValueFrame.class, Optional.empty(), spark, BUILDER);
  }

}
