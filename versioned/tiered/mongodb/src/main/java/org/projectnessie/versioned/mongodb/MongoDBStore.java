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

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.projectnessie.versioned.impl.EntityStoreHelper;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateClauseVisitor;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadOp;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.StoreOperationException;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class implements the Store interface that is used by Nessie as a backing store for versioning of it's
 * Git like behaviour.
 * The MongoDbStore connects to an external MongoDB server.
 */
public class MongoDBStore implements Store {
  /**
   * Pair of a collection to the set of IDs to be loaded.
   */
  private static class CollectionLoadIds {
    final ValueType<?> type;
    final MongoCollection<Document> collection;
    final List<Id> ids;

    CollectionLoadIds(ValueType<?> type, MongoCollection<Document> collection, List<Id> ops) {
      this.type = type;
      this.collection = collection;
      this.ids = ops;
    }
  }

  // Mongo has a 16MB limit on documents, which also pertains to the input query. Given that we use IN for loads,
  // restrict the number of IDs to avoid going above that limit, and to take advantage of the async nature of the
  // requests.
  @VisibleForTesting
  static final int LOAD_SIZE = 1_000;

  private final MongoStoreConfig config;
  private final MongoClientSettings mongoClientSettings;

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private final Duration timeout;
  private Map<ValueType<?>, MongoCollection<Document>> collections;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param config the configuration for the store.
   */
  public MongoDBStore(MongoStoreConfig config) {
    this.config = config;
    this.timeout = Duration.ofMillis(config.getTimeoutMs());
    this.collections = new HashMap<>();
    this.mongoClientSettings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(config.getConnectionString()))
      .codecRegistry(CodecRegistries.fromProviders(
          new CodecProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
              return clazz == Id.class ? (Codec<T>) ID_CODEC_INSTANCE : null;
            }
          },
          MongoClientSettings.getDefaultCodecRegistry()))
      .writeConcern(WriteConcern.MAJORITY)
      .build();
  }

  /**
   * Gets a handle to an existing database or get a handle to a MongoDatabase instance if it does not exist. The new
   * database will be lazily created.
   * Since MongoDB creates databases and collections if they do not exist, there is no need to validate the presence of
   * either before they are used. This creates or retrieves collections that map 1:1 to the enumerates in
   * {@link org.projectnessie.versioned.store.ValueType}
   */
  @Override
  public void start() {
    mongoClient = MongoClients.create(mongoClientSettings);
    mongoDatabase = mongoClient.getDatabase(config.getDatabaseName());

    // Initialise collections for each ValueType.
    collections = ValueType.values().stream()
        .collect(ImmutableMap.<ValueType<?>, ValueType<?>, MongoCollection<Document>>toImmutableMap(
            v -> v,
            v -> {
              String collectionName = v.getTableName(config.getTablePrefix());
              return mongoDatabase.getCollection(collectionName);
            })
        );

    if (config.initializeDatabase()) {
      // make sure we have an empty l1 (ignore result, doesn't matter)
      EntityStoreHelper.storeMinimumEntities(this::putIfAbsent);
    }
  }

  /**
   * Closes the connection this manager creates to a database. If the connection is already closed this method has
   * no effect.
   */
  @Override
  public void close() {
    if (null != mongoClient) {
      mongoClient.close();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void load(LoadStep loadstep) throws NotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final Map<Id, LoadOp<?>> idLoadOps = step.getOps().collect(Collectors.toMap(LoadOp::getId, Function.identity()));

      Flux.fromStream(step.getOps())
        .groupBy(LoadOp::getValueType)
        .flatMap(entry -> {
          ValueType<?> type = entry.key();

          MongoCollection<Document> collection = getCollection(type);

          return entry.map(LoadOp::getId)
              .buffer(LOAD_SIZE)
              .map(l -> new CollectionLoadIds(type, collection, l));
        })
          .flatMap(entry -> entry.collection.find(Filters.in(MongoBaseValue.ID, entry.ids)))
          .handle((op, sink) -> {
            // Process each of the loaded entries.
            Id id = MongoSerDe.deserializeId(op, MongoBaseValue.ID);
            LoadOp loadOp = idLoadOps.remove(id);
            MongoSerDe.produceToConsumer(op, loadOp.getValueType(), loadOp.getReceiver());
            loadOp.done();
          })
          .blockLast(timeout);

      // Check if there were any missed ops.
      final Collection<String> missedIds = idLoadOps.values().stream()
          .map(e -> e.getId().toString())
          .collect(Collectors.toList());
      if (!missedIds.isEmpty()) {
        throw new NotFoundException(String.format("Requested object IDs missing: %s", String.join(", ", missedIds)));
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    final MongoCollection<Document> collection = getCollection(saveOp.getType());

    // Use upsert so that a document is created if the filter does not match. The update operator is only $setOnInsert
    // so no action is triggered on a simple update, only on insert.
    final UpdateResult result = Mono.from(collection.updateOne(
        Filters.eq(MongoBaseValue.ID, saveOp.getId()),
        MongoSerDe.bsonForValueType(saveOp, "$setOnInsert"),
        new UpdateOptions().upsert(true)
    )).block(timeout);
    return result != null && result.getUpsertedId() != null;
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> conditionUnAliased) {
    Bson filter = Filters.eq(Store.KEY_NAME, saveOp.getId());
    if (conditionUnAliased.isPresent()) {
      filter = Filters.and(filter, transform(conditionUnAliased.get()));
    }

    final MongoCollection<Document> collection = getCollection(saveOp.getType());

    // Use upsert so that if an item does not exist, it will be inserted.
    final UpdateResult result = Mono.from(
        collection.updateOne(
            filter,
            MongoSerDe.bsonForValueType(saveOp, "$set"),
            new UpdateOptions().upsert(!conditionUnAliased.isPresent())
        )).block(timeout);
    if (conditionUnAliased.isPresent()) {
      if (result == null || result.getMatchedCount() == 0) {
        throw new ConditionFailedException(String.format("Update of %s %s did not succeed", saveOp.getType().name(), saveOp.getId()));
      }
    } else if (result == null || (result.getModifiedCount() != 0 && result.getUpsertedId() == null)) {
      throw new StoreOperationException(String.format("Update of %s %s did not succeed", saveOp.getType().name(), saveOp.getId()));
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    final MongoCollection<?> collection = getCollection(type);

    Bson filter = Filters.eq(Store.KEY_NAME, id);
    if (condition.isPresent()) {
      filter = Filters.and(filter, transform(condition.get()));
    }

    return 0 != Mono.from(collection.deleteOne(filter)).block(timeout).getDeletedCount();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void save(List<SaveOp<?>> ops) {
    Map<ValueType<?>, List<SaveOp>> perType = ops.stream()
        .collect(Collectors.groupingBy(SaveOp::getType));

    Flux.fromIterable(perType.entrySet())
      .flatMap(entry -> ((MongoCollection) writeCollection(entry.getKey()))
          .insertMany(entry.getValue(), new InsertManyOptions().ordered(false)))
        .blockLast(timeout);
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> valueType, Id id, C consumer) {
    final MongoCollection<Document> collection = getCollection(valueType);

    Document found = Mono.from(
        collection.find(Filters.eq(MongoBaseValue.ID, id))
    ).block(timeout);

    if (null == found) {
      throw new NotFoundException(String.format("Unable to load item with ID: %s", id));
    }

    MongoSerDe.produceToConsumer(found, valueType, consumer);
  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    final MongoCollection<Document> collection = getCollection(type);

    Bson filter = Filters.eq(Store.KEY_NAME, id);
    if (condition.isPresent()) {
      filter = Filters.and(filter, transform(condition.get()));
    }

    final Mono<Document> updateMono;
    if (shouldUseAggPipeline(update)) {
      // Mongo cannot remove an element from an array in one operation, it requires two or the agg pipeline. Thus,
      // when we detect that case use the agg pipeline instead of the normal pipeline. Note that this is likely to be
      // slower than the normal path, but given that it's used for cleanup purposes and is not on the critical path,
      // this should be fine.
      updateMono = Mono.from(collection.findOneAndUpdate(
        filter,
        transformAgg(update),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    } else {
      updateMono = Mono.from(collection.findOneAndUpdate(
        filter,
        transform(update),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }

    final Optional<Document> updated = updateMono.blockOptional(timeout);
    consumer.ifPresent(c -> updated.ifPresent(u -> MongoSerDe.produceToConsumer(u, type, c)));
    return updated.isPresent();
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    // TODO: Can this be optimized to not collect the elements before streaming them?
    // TODO: Could this benefit from paging?
    return Flux.from(this.getCollection(type).find()).toStream()
        .map(d -> producer -> MongoSerDe.produceToConsumer(d, type, producer));
  }

  /**
   * Clear the contents of all the Nessie collections. Only for testing purposes.
   */
  @VisibleForTesting
  void resetCollections() {
    Flux.fromIterable(collections.values()).flatMap(collection -> collection.deleteMany(Filters.ne("_id", "s"))).blockLast(timeout);
  }

  @SuppressWarnings("rawtypes")
  private MongoCollection<Document> writeCollection(ValueType<?> type) {
    Codec<SaveOp> codec = new Codec<SaveOp>() {
      @Override
      public SaveOp decode(BsonReader bsonReader, DecoderContext decoderContext) {
        throw new UnsupportedOperationException();
      }

      @SuppressWarnings("unchecked")
      @Override
      public void encode(BsonWriter bsonWriter, SaveOp o, EncoderContext encoderContext) {
        MongoSerDe.serializeEntity(bsonWriter, o);
      }

      @Override
      public Class<SaveOp> getEncoderClass() {
        return SaveOp.class;
      }
    };

    return this.getCollection(type, codec);
  }

  private MongoCollection<Document> getCollection(ValueType<?> valueType, Codec<?> codec) {
    return getCollection(valueType).withCodecRegistry(
        new CodecRegistry() {
          @Override
          public <T> Codec<T> get(Class<T> clazz, CodecRegistry codecRegistry) {
            return get(clazz);
          }

          @SuppressWarnings("unchecked")
          @Override
          public <T> Codec<T> get(Class<T> clazz) {
            return (Codec<T>) (clazz == Id.class ? ID_CODEC_INSTANCE : codec);
          }
        });
  }

  private MongoCollection<Document> getCollection(ValueType<?> valueType) {
    return checkNotNull(collections.get(valueType), "Unsupported Entity type: %s", valueType.name());
  }

  /**
   * Transform the ConditionExpression into a BSON representation of the ConditionExpression object.
   * @param conditionExpression to be converted into BSON.
   * @return the converted ConditionExpression object in BSON format.
   */
  @VisibleForTesting
  static Bson transform(final ConditionExpression conditionExpression) {
    final String functions = conditionExpression.getFunctions().stream()
        .map(f -> f.accept(BsonValueVisitor.VALUE_VISITOR))
        .collect(Collectors.joining(", "));

    // This intentionally builds up a JSON representation of the query, and then parses it back to a BSON Object
    // representation. The reason for this is that the Value hierarchy in Nessie is a combination of Bson and BsonValue
    // in MongoDB parlance, and those objects don't share a common parent, which makes the standard visitor need to
    // return both.
    return BsonDocument.parse(String.format("{\"$and\": [%s]}", functions));
  }

  @VisibleForTesting
  static Bson transform(UpdateExpression expression) {
    final ImmutableList.Builder<Bson> builder = ImmutableList.builder();
    expression.getClauses().forEach(c -> builder.add(c.accept(BsonUpdateVisitor.CLAUSE_VISITOR)));
    return Updates.combine(builder.build());
  }

  @VisibleForTesting
  static List<Bson> transformAgg(UpdateExpression expression) {
    // Sort the operations so the array removals occur in reverse order to avoid indexing problems. Since we use a pipeline,
    // if they occur in increasing order then consider the case of: [a, b, c] with a removal of 0, 1. You would expect
    // a return array of [c], however what occurs is actually:
    //  - remove 0: [a, b, c] -> [b, c]
    //  - remove 1: [b, c] -> [b]
    // if instead they occur in reverse order:
    //  - remove 1: [a, b, c] -> [a, c]
    //  - remove 0: [a, c] -> [c]
    // TODO: opportunity to optimize the removals by collapsing sequential removed indexes into ranges.
    return expression.getClauses().stream()
      .map(c -> c.accept(BsonAggUpdateVisitor.CLAUSE_VISITOR))
      .sorted((o1, o2) -> {
        final int compare = o2.path.compareTo(o1.path);
        return (compare == 0) ? o2.index.compareTo(o1.index) : compare;
      })
      .map(o -> BsonDocument.parse(o.operation))
      .collect(Collectors.toList());
  }

  private boolean shouldUseAggPipeline(UpdateExpression update) {
    return update.getClauses().stream().anyMatch(e -> e.accept(new UpdateClauseVisitor<Boolean>() {
      @Override
      public Boolean visit(RemoveClause clause) {
        // If there is a remove clause that operates on an array element, this must use the agg pipeline.
        return BsonUpdateVisitor.isArrayPath(clause.getPath());
      }

      @Override
      public Boolean visit(SetClause clause) {
        return false;
      }
    }));
  }

  private static class IdCodec implements Codec<Id> {
    @Override
    public Id decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return Id.of(bsonReader.readBinaryData().getData());
    }

    @Override
    public void encode(BsonWriter bsonWriter, Id id, EncoderContext encoderContext) {
      bsonWriter.writeBinaryData(new BsonBinary(id.toBytes()));
    }

    @Override
    public Class<Id> getEncoderClass() {
      return Id.class;
    }
  }

  static final IdCodec ID_CODEC_INSTANCE = new IdCodec();
}
