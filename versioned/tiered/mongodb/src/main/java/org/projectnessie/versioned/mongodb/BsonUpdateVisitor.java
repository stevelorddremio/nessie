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
package org.projectnessie.versioned.mongodb;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateClauseVisitor;
import org.projectnessie.versioned.store.Entity;

import com.mongodb.client.model.Updates;

/**
 * This provides a separation of generation of BSON expressions from @{UpdateClause} from the object itself.
 */
class BsonUpdateVisitor implements UpdateClauseVisitor<Bson> {
  private static final String PUSH_OP = "$push";
  private static final String SET_OP = "$set";
  private static final String EACH_OP = "$each";

  /**
   * Represent a BSON document for update operations.
   * Intentionally don't use Updates.*() as that will result in issues encoding the value entity,
   * due to codec lookups the MongoDB driver will actually encode the fields of the basic object, not the entity.
   */
  private static class UpdateBson implements Bson {
    private final String operation;
    private final String path;
    private final Entity value;

    UpdateBson(String operation, String path, Entity value) {
      this.operation = operation;
      this.path = path;
      this.value = value;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> clazz, CodecRegistry codecRegistry) {
      final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
      writer.writeStartDocument();
      writer.writeName(operation);
      writer.writeStartDocument();
      writer.writeName(path);
      if (PUSH_OP.equals(operation) && value.getType().equals(Entity.EntityType.LIST)) {
        writer.writeStartDocument();
        serializeWithName(writer, EACH_OP, value);
        writer.writeEndDocument();
      } else {
        serialize(writer, value);
      }
      writer.writeEndDocument();
      writer.writeEndDocument();
      return writer.getDocument();
    }

    private void serializeWithName(BsonDocumentWriter writer, String name, Entity entity) {
      writer.writeName(name);
      serialize(writer, entity);
    }

    private void serialize(BsonDocumentWriter writer, Entity entity) {
      switch (entity.getType()) {
        case MAP:
          writer.writeStartDocument();
          entity.getMap().forEach((k, v) -> serializeWithName(writer, k, v));
          writer.writeEndDocument();
          break;
        case LIST:
          writer.writeStartArray();
          entity.getList().forEach(v -> serialize(writer, v));
          writer.writeEndArray();
          break;
        case STRING:
          writer.writeString(entity.getString());
          break;
        case BOOLEAN:
          writer.writeBoolean(entity.getBoolean());
          break;
        case NUMBER:
          writer.writeInt64(entity.getNumber());
          break;
        case BINARY:
          writer.writeBinaryData(new BsonBinary(entity.getBinary().toByteArray()));
          break;
        default:
          throw new UnsupportedOperationException(entity.getType().toString());
      }
    }
  }

  static final BsonUpdateVisitor CLAUSE_VISITOR = new BsonUpdateVisitor();

  @Override
  public Bson visit(RemoveClause clause) {
    final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);
    if (isArrayPath(clause.getPath())) {
      // Mongo has no way of deleting an element from an array in on operation, it leaves a NULL element instead.
      // Use the AggBsonUpdateVisitor instead of the BsonUpdateVisitor to achieve this.
      throw new UnsupportedOperationException("Array element removal is not supported without the aggregation pipeline.");
    }

    return Updates.unset(path);
  }

  @Override
  public Bson visit(SetClause clause) {
    final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);

    switch (clause.getValue().getType()) {
      case VALUE:
        return new UpdateBson(SET_OP, path, clause.getValue().getValue());
      case FUNCTION:
        return handleFunction(path, clause.getValue().getFunction());
      default:
        throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
    }
  }

  private Bson handleFunction(String path, ExpressionFunction function) {
    if (ExpressionFunction.FunctionName.LIST_APPEND == function.getName()) {
      return new UpdateBson(PUSH_OP, path, function.getArguments().get(1).getValue());
    }

    throw new UnsupportedOperationException(String.format("Unsupported Set function: %s", function.getName()));
  }

  /**
   * Determine whether or not the given ExpressionPath refers to an array element.
   * @param path the path to check.
   * @return true if the path is for an array element, false otherwise.
   */
  static boolean isArrayPath(ExpressionPath path) {
    ExpressionPath.PathSegment segment = path.getRoot();
    while (segment.getChild().isPresent()) {
      segment = segment.getChild().get();
    }

    return segment.isPosition();
  }
}
