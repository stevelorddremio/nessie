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
package org.projectnessie.versioned.dynamodb;

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.attributeValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeId;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeKey;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.idValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.keyElements;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.list;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.map;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.L3;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoL3 extends DynamoBaseValue<L3> implements L3 {

  static final String TREE = "tree";
  static final String TREE_KEY = "key";
  static final String TREE_ID = "id";

  DynamoL3() {
    super(ValueType.L3);
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    return addEntitySafe(TREE, list(keyDelta.map(DynamoL3::treeKeyId)));
  }

  private static AttributeValue treeKeyId(KeyDelta kd) {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(DynamoL3.TREE_KEY, keyElements(kd.getKey()));
    map.put(DynamoL3.TREE_ID, idValue(kd.getId()));
    return map(map);
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(TREE, "keyDelta");

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void toConsumer(Map<String, AttributeValue> entity, L3 consumer) {
    baseToConsumer(entity, consumer);

    if (entity.containsKey(TREE)) {
      Stream<KeyDelta> keyDelta = attributeValue(entity, TREE).l().stream()
          .map(AttributeValue::m)
          .map(m -> KeyDelta.of(
              deserializeKey(attributeValue(m, TREE_KEY)),
              deserializeId(m, TREE_ID)
          ));

      consumer.keyDelta(keyDelta);
    }
  }
}
