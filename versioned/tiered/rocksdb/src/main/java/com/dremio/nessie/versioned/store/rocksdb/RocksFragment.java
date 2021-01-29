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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;

public class RocksFragment extends RocksBaseValue<Fragment> implements Fragment, Evaluator {

  static final String KEY_LIST = "keys";
  static RocksFragment EMPTY = new RocksFragment();
  static Id EMPTY_ID = EMPTY.getId();

  List<Entity> keyList;

  public RocksFragment() {
    super(EMPTY_ID, 0L);
    keyList = new ArrayList<>();
  }

  @Override
  public boolean evaluate(Condition condition) {
    boolean result = true;
    for (Function function: condition.functionList) {
      // Retrieve entity at function.path
      List<String> path = function.getPathAsList();
      String segment = path.get(0);
      if (segment.equals(ID)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (getId().toEntity().equals(function.getValue())));
      } else if (segment.equals(KEY_LIST)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
        && (Entity.ofList()keyList));
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }

  // TODO: Is there a better way to do this with streams?
  @Override
  public Fragment keys(Stream<Key> keys) {
    List<Key> _keys = keys.collect(Collectors.toList());
    for (Key key: _keys) {
      key.getElements().stream().collect(Collectors.append())
      Entity.ofList()key.getElements()
      keyList.add(key.toString());
    }
    return this;
  }
}
