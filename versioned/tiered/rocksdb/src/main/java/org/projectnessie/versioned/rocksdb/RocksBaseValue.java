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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.UpdateClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * An implementation of @{BaseValue} used for ConditionExpression and UpdateExpression evaluation.
 * @param <C> Specialization of a specific BaseValue interface.
 */
abstract class RocksBaseValue<C extends BaseValue<C>> implements BaseValue<C>, Evaluator, Updater {

  static final String ID = "id";
  private final ValueProtos.BaseValue.Builder builder = ValueProtos.BaseValue.newBuilder();

  RocksBaseValue() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public C id(Id id) {
    builder.setId(id.getValue());
    return (C) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public C dt(long dt) {
    builder.setDatetime(dt);
    return (C) this;
  }

  protected ValueProtos.BaseValue buildBase() {
    return builder.build();
  }

  /**
   * Consume the base value attributes.
   * @param consumer the base value consumer.
   * @param base the protobuf base value object.
   */
  static <C extends BaseValue<C>> void setBase(BaseValue<C> consumer, ValueProtos.BaseValue base) {
    consumer.id(Id.of(base.getId()));
    consumer.dt(base.getDatetime());
  }

  /**
   * Converts a Stream of Ids into a List Entity.
   * @param idList the stream of Id to convert.
   * @return the List Entity.
   */
  static Entity toEntity(List<Id> idList) {
    return Entity.ofList(idList.stream().map(Id::toEntity).collect(Collectors.toList()));
  }

  /**
   * Retrieves an Id at 'position' in a Stream of Ids as an Entity.
   * @param idList the stream of Id to convert.
   * @param position the element in the Stream to retrieve.
   * @return the List Entity.
   */
  static Entity toEntity(List<Id> idList, int position) {
    return idList.stream().skip(position).findFirst().orElseThrow(NoSuchElementException::new).toEntity();
  }

  /**
   * Evaluates if the idList of Id meets the Condition Function.
   * @param function the function of the ConditionExpression to evaluate.
   * @param idList The Ids to evaluate against the function
   * @return true if the condition is met
   */
  boolean evaluate(Function function, List<Id> idList) {
    // EQUALS will either compare a specified position or the whole idList as a List.
    if (function.getOperator().equals(Function.Operator.EQUALS)) {
      final ExpressionPath.PathSegment pathSegment = function.getPath().getRoot().getChild().orElse(null);
      if (pathSegment == null) {
        return toEntity(idList).equals(function.getValue());
      } else if (pathSegment.isPosition()) { // compare individual element of list
        final int position = pathSegment.asPosition().getPosition();
        return toEntity(idList, position).equals(function.getValue());
      }
    } else if (function.getOperator().equals(Function.Operator.SIZE)) {
      return (idList.size() == function.getValue().getNumber());
    }

    return false;
  }

  @Override
  public void evaluate(List<Function> functions) throws ConditionFailedException {
    for (Function function: functions) {
      if (function.getPath().getRoot().isName()) {
        try {
          evaluate(function);
        } catch (IllegalStateException e) {
          // Catch exceptions raise due to incorrect Entity type in FunctionExpression being compared to the
          // target attribute.
          throw new ConditionFailedException(invalidValueMessage(function));
        }
      }
    }
  }

  /**
   * Checks that a Function is met by the implementing class.
   * @param function the condition to check
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  public abstract void evaluate(Function function) throws ConditionFailedException;

  /**
   * Evaluates the id against a function and ensures that the name segment is well formed,
   * ie does not contain a child attribute.
   * @param function the function to evaluate against id.
   * @throws ConditionFailedException thrown if the condition expression is invalid or the condition is not met.
   */
  void evaluatesId(Function function) throws ConditionFailedException {
    if (!function.isRootNameSegmentChildlessAndEquals()
        || !getId().toEntity().equals(function.getValue())) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }
  }

  @VisibleForTesting
  Id getId() {
    return Id.of(buildBase().getId());
  }

  protected static String invalidOperatorSegmentMessage(Function function) {
    return String.format("Operator: %s is not applicable to segment %s",
      function.getOperator(), function.getRootPathAsNameSegment().getName());
  }

  protected static String invalidValueMessage(Function function) {
    return String.format("Not able to apply type: %s to segment: %s", function.getValue().getType(),
      function.getRootPathAsNameSegment().getName());
  }

  protected static String conditionNotMatchedMessage(Function function) {
    return String.format("Condition %s did not match the actual value for %s", function.getValue().getType(),
      function.getRootPathAsNameSegment().getName());
  }

  /**
   * Entry point to update this object with a series of UpdateClauses.
   * @param updates the updates to apply
   */
  @Override
  public void update(UpdateExpression updates) {
    List<ExpressionPath.PathSegment> removedPaths = updates.getClauses()
        .stream()
        .map(this::updateWithClause)
        .filter(Objects::nonNull)
        .sorted(new Comparator<ExpressionPath.PathSegment>() {
          /* Sort the PathSegments as follows
           * names before positions
           * names by string order
           * higher positions before lower positions
           * path with child before path without child
           * when equal, recurse in with the child segments
           */
          @Override
          public int compare(ExpressionPath.PathSegment ps1, ExpressionPath.PathSegment ps2) {
            if (ps1.isName() && ps2.isName()) {
              if (ps1.asName().getName().equals(ps2.asName().getName())) {
                if (!ps1.getChild().isPresent() && ps2.getChild().isPresent()) {
                  return 1;
                } else if (ps1.getChild().isPresent() && !ps2.getChild().isPresent()) {
                  return -1;
                } else if (!ps1.getChild().isPresent() && !ps2.getChild().isPresent()) {
                  return 0;
                } else {
                  return compare(ps1.getChild().get(), ps2.getChild().get());
                }
              } else {
                return ps1.asName().getName().compareTo(ps2.asName().getName());
              }
            } else if (ps1.isName()) {
              return -1;
            } else if (ps1.isPosition() && ps2.isPosition()) {
              return ps2.asPosition().getPosition() - ps1.asPosition().getPosition();
            } else {
              return 1;
            }
          }
        }).collect(Collectors.toList());

    for (ExpressionPath.PathSegment removedPath : removedPaths) {
      if (removedPath.getChild().isPresent()) {
        remove(removedPath.asName().getName(), removedPath.getChild().get());
      }
    }
  }

  // TODO: we might be able to provide implementation here and just have
  // abstract methods for SET and REMOVE, which will be implemented by the
  // specific classes L1, L2 etc.
  /**
   * Applies an update to the implementing class.
   * @param updateClause the update to apply to the implementing class.
   * @return The PathSegment that should be removed, or null
   */
  private ExpressionPath.PathSegment updateWithClause(UpdateClause updateClause) {
    final UpdateFunction updateFunction = updateClause.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR);
    final ExpressionPath.NameSegment nameSegment = updateFunction.getRootPathAsNameSegment();
    final String segment = nameSegment.getName();
    final ExpressionPath.PathSegment childPath = nameSegment.getChild().orElse(null);

    switch (updateFunction.getOperator()) {
      case REMOVE:
        if (childPath == null || !fieldIsList(segment, childPath)) {
          throw new UnsupportedOperationException();
        }

        return nameSegment;
      case SET:
        UpdateFunction.SetFunction setFunction = (UpdateFunction.SetFunction) updateFunction;
        switch (setFunction.getSubOperator()) {
          case APPEND_TO_LIST:
            if (fieldIsList(segment, childPath)) {
              if (setFunction.getValue().getType() == Entity.EntityType.LIST) {
                appendToList(segment, childPath, setFunction.getValue().getList());
              } else {
                appendToList(segment, childPath, Collections.singletonList(setFunction.getValue()));
              }
            } else {
              throw new UnsupportedOperationException();
            }
            break;
          case EQUALS:
            if (fieldIsList(segment, childPath) && childPath != null && childPath.isPosition()) {
              set(segment, childPath, setFunction.getValue());
            } else if (fieldIsList(segment, childPath) != (setFunction.getValue().getType() == Entity.EntityType.LIST)) {
              throw new UnsupportedOperationException();
            } else if (ID.equals(segment)) {
              id(Id.of(setFunction.getValue().getBinary()));
            } else {
              set(segment, childPath, setFunction.getValue());
            }
            break;
          default:
            throw new UnsupportedOperationException();
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }

    return null;
  }

  protected int getPosition(ExpressionPath.PathSegment path) {
    if (path != null && path.isPosition()) {
      return path.asPosition().getPosition();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  protected abstract void remove(String fieldName, ExpressionPath.PathSegment path);

  protected abstract boolean fieldIsList(String fieldName, ExpressionPath.PathSegment childPath);

  protected abstract void appendToList(String fieldName, ExpressionPath.PathSegment childPath, List<Entity> valuesToAdd);

  protected abstract void set(String fieldName, ExpressionPath.PathSegment childPath, Entity newValue);

  /**
   * Serialize the value to protobuf format.
   * @return the value serialized as protobuf.
   */
  abstract byte[] build();

  static ValueProtos.Key buildKey(Key key) {
    return ValueProtos.Key.newBuilder().addAllElements(key.getElements()).build();
  }

  static ValueProtos.KeyMutation buildKeyMutation(Key.Mutation m) {
    return ValueProtos.KeyMutation.newBuilder().setType(getType(m)).setKey(buildKey(m.getKey())).build();
  }

  private static ValueProtos.KeyMutation.MutationType getType(Key.Mutation mutation) {
    switch (mutation.getType()) {
      case ADDITION:
        return ValueProtos.KeyMutation.MutationType.ADDITION;
      case REMOVAL:
        return ValueProtos.KeyMutation.MutationType.REMOVAL;
      default:
        throw new IllegalArgumentException("Unknown mutation type " + mutation.getType());
    }
  }

  static Key createKey(ValueProtos.Key key) {
    return Key.of(key.getElementsList().toArray(new String[0]));
  }

  static Key.Mutation createKeyMutation(ValueProtos.KeyMutation k) {
    switch (k.getType()) {
      case ADDITION:
        return createKey(k.getKey()).asAddition();
      case REMOVAL:
        return createKey(k.getKey()).asRemoval();
      default:
        throw new IllegalArgumentException("Unknown mutation type " + k.getType());
    }
  }

  <E> void checkPresent(E element, String name) {
    Preconditions.checkArgument(
        null != element,
        String.format("Method %s of consumer %s has not been called", name, getClass().getSimpleName()));
  }
}
