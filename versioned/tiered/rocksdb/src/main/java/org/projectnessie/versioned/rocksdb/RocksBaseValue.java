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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
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
  static final String DATETIME = "dt";

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
    final List<UpdateFunction> updatesToApply = removeUnnecessaryFunctions(updates.getClauses());

    updatesToApply
        .stream()
        .filter(uf -> uf.getOperator() != UpdateFunction.Operator.REMOVE)
        .forEach(this::updateWithFunction);

    updatesToApply
        .stream()
        .filter(uf -> uf.getOperator() == UpdateFunction.Operator.REMOVE)
        .map(uf -> uf.getPath())
        .sorted(new Comparator<ExpressionPath>() {
          /* Sort the PathSegments as follows
           * names before positions
           * names by string order
           * higher positions before lower positions
           * path with child before path without child
           * when equal, recurse in with the child segments
           */
          @Override
          public int compare(ExpressionPath path1, ExpressionPath path2) {
            return compareSegments(path1.getRoot(), path2.getRoot());
          }
        }).forEach(rp -> {
          remove(rp);
        });
  }

  private static int compareSegments(ExpressionPath.PathSegment ps1, ExpressionPath.PathSegment ps2) {
    if (ps1.isName() && ps2.isName()) {
      if (ps1.asName().getName().equals(ps2.asName().getName())) {
        if (!ps1.getChild().isPresent() && ps2.getChild().isPresent()) {
          return 1;
        } else if (ps1.getChild().isPresent() && !ps2.getChild().isPresent()) {
          return -1;
        } else if (!ps1.getChild().isPresent() && !ps2.getChild().isPresent()) {
          return 0;
        } else {
          return compareSegments(ps1.getChild().get(), ps2.getChild().get());
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

  /**
   * Filters through the list of UpdateClause objects to remove conflicting updates. The order of
   * the updates is preserved. Conflicts are defined as follows:
   * <ul>
   *   <li>Remove -&gt; Remove conflicts if latter remove is same node or a child node of former remove</li>
   *   <li>Remove -&gt; SetEquals conflicts if SetEquals is for the same node or a child node of Remove</li>
   *   <li>Remove -&gt; SetEquals conflicts if Remove is for a child node of SetEquals</li>
   *   <li>Remove -&gt; Append conflicts if Append is for the same node or a child node of Remove</li>
   *   <li>SetEquals -&gt; Remove conflicts if Remove is for a child node of SetEquals</li>
   *   <li>SetEquals -&gt; SetEquals conflicts if latter SetEquals is for child node of the former SetEquals</li>
   *   <li>SetEquals -&gt; Append conflicts if Append is for the same node or a child node of SetEquals</li>
   * </ul>
   * @param updateClauses The list of UpdateClause objects to filter
   * @return A safe list of UpdateFunction objects to apply
   */
  private List<UpdateFunction> removeUnnecessaryFunctions(List<UpdateClause> updateClauses) {
    final List<UpdateFunction> updateFunctions = updateClauses
        .stream()
        .map(uc -> uc.accept(RocksDBUpdateClauseVisitor.ROCKS_DB_UPDATE_CLAUSE_VISITOR))
        .collect(Collectors.toList());
    final boolean[] keepUpdateFunction = new boolean[updateFunctions.size()];
    Arrays.fill(keepUpdateFunction, true);

    for (int i = 1; i < updateClauses.size(); i++) {
      final UpdateFunction currentUpdateFunction = updateFunctions.get(i);
      final String currentUpdatePath = currentUpdateFunction.getPath().asString();
      final UpdateFunction.Operator currentOperator = currentUpdateFunction.getOperator();
      final UpdateFunction.SetFunction.SubOperator currentSubOperator;
      if (currentOperator == UpdateFunction.Operator.SET) {
        currentSubOperator = ((UpdateFunction.SetFunction) currentUpdateFunction).getSubOperator();
      } else {
        currentSubOperator = null;
      }

      for (int j = 0; j < i; j++) {
        if (!keepUpdateFunction[j]) {
          continue;
        }

        final UpdateFunction earlierUpdateFunction = updateFunctions.get(j);
        final String earlierUpdatePath = earlierUpdateFunction.getPath().asString();
        final UpdateFunction.Operator earlierOperator = earlierUpdateFunction.getOperator();
        final UpdateFunction.SetFunction.SubOperator earlierSubOperator;
        if (earlierOperator == UpdateFunction.Operator.SET) {
          earlierSubOperator = ((UpdateFunction.SetFunction) earlierUpdateFunction).getSubOperator();
        } else {
          earlierSubOperator = null;
        }

        if (currentOperator == UpdateFunction.Operator.REMOVE
            && earlierOperator == UpdateFunction.Operator.REMOVE) {
          if (currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          }
        } else if (earlierOperator == UpdateFunction.Operator.REMOVE
            && currentOperator == UpdateFunction.Operator.SET
            && currentSubOperator == UpdateFunction.SetFunction.SubOperator.EQUALS) {
          if (currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          } else if (earlierUpdatePath.startsWith(currentUpdatePath)) {
            keepUpdateFunction[j] = false;
          }
        } else if (earlierOperator == UpdateFunction.Operator.REMOVE
            && currentOperator == UpdateFunction.Operator.SET
            && currentSubOperator == UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST) {
          if (currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          }
        } else if (earlierOperator == UpdateFunction.Operator.SET
            && earlierSubOperator == UpdateFunction.SetFunction.SubOperator.EQUALS
            && currentOperator == UpdateFunction.Operator.REMOVE) {
          if (!currentUpdatePath.equals(earlierUpdatePath) && currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          }
        } else if (earlierOperator == UpdateFunction.Operator.SET
            && earlierSubOperator == UpdateFunction.SetFunction.SubOperator.EQUALS
            && currentOperator == UpdateFunction.Operator.SET
            && currentSubOperator == UpdateFunction.SetFunction.SubOperator.EQUALS) {
          if (!currentUpdatePath.equals(earlierUpdatePath) && currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          }
        } else if (earlierOperator == UpdateFunction.Operator.SET
            && earlierSubOperator == UpdateFunction.SetFunction.SubOperator.EQUALS
            && currentOperator == UpdateFunction.Operator.SET
            && currentSubOperator == UpdateFunction.SetFunction.SubOperator.APPEND_TO_LIST) {
          if (currentUpdatePath.startsWith(earlierUpdatePath)) {
            keepUpdateFunction[i] = false;
          }
        }
      }
    }

    final List<UpdateFunction> functionsToApply = new ArrayList<>();
    for (int i = 0; i < keepUpdateFunction.length; i++) {
      if (keepUpdateFunction[i]) {
        functionsToApply.add(updateFunctions.get(i));
      }
    }
    return functionsToApply;
  }

  // TODO: we might be able to provide implementation here and just have
  // abstract methods for SET and REMOVE, which will be implemented by the
  // specific classes L1, L2 etc.
  /**
   * Applies an update to the implementing class.
   * @param updateFunction the update to apply to the implementing class.
   * @return The PathSegment that should be removed, or null
   */
  private void updateWithFunction(UpdateFunction updateFunction) {
    final ExpressionPath.NameSegment nameSegment = updateFunction.getRootPathAsNameSegment();
    final ExpressionPath.PathSegment childPath = nameSegment.getChild().orElse(null);

    switch (updateFunction.getOperator()) {
      case REMOVE:
        if (childPath == null || !fieldIsList(updateFunction.getPath())) {
          throw new UnsupportedOperationException(
            String.format("Expression path \"%s\" is not valid", updateFunction.getPath().asString())
          );
        }

        return;
      case SET:
        updateWithSetFunction((UpdateFunction.SetFunction) updateFunction);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unknown operation \"%s\"", updateFunction.getOperator()));
    }
  }

  private void updateWithSetFunction(UpdateFunction.SetFunction setFunction) {
    switch (setFunction.getSubOperator()) {
      case APPEND_TO_LIST:
        if (!fieldIsList(removeLastPositionSegmentIfPosition(setFunction.getPath()))) {
          throw new UnsupportedOperationException(
              String.format("The append to list operation is not valid for \"%s\"", setFunction.getPath().asString()));
        }

        final List<Entity> valuesToAppend;
        if (setFunction.getValue().getType() == Entity.EntityType.LIST) {
          valuesToAppend = setFunction.getValue().getList();
        } else {
          valuesToAppend = Collections.singletonList(setFunction.getValue());
        }

        appendToList(setFunction.getPath(), valuesToAppend);
        break;
      case EQUALS:
        final ExpressionPath fieldPath = removeLastPositionSegmentIfPosition(setFunction.getPath());

        if (fieldIsList(fieldPath)) {
          set(setFunction.getPath(), setFunction.getValue());
        } else if (fieldIsList(fieldPath) != (setFunction.getValue().getType() == Entity.EntityType.LIST)) {
          throw new UnsupportedOperationException(
            String.format("The value for \"%s\" must be a list", setFunction.getPath().asString())
          );
        } else if (ID.equals(setFunction.getPath().getRoot().getName())) {
          id(Id.of(setFunction.getValue().getBinary()));
        } else if (DATETIME.equals(setFunction.getPath().getRoot().getName())) {
          dt(setFunction.getValue().getNumber());
        } else {
          set(setFunction.getPath(), setFunction.getValue());
        }
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unknown sub operation \"%s\"", setFunction.getSubOperator().name()));
    }
  }

  private ExpressionPath removeLastPositionSegmentIfPosition(ExpressionPath path) {
    ExpressionPath.PathSegment currentNode = path.getRoot();
    ExpressionPath.PathSegment.Builder pathBuilder = ExpressionPath.builder(currentNode.asName().getName());
    currentNode = currentNode.getChild().orElse(null);

    while (currentNode != null && (currentNode.getChild().isPresent() || currentNode.isName())) {
      if (currentNode.isPosition()) {
        pathBuilder = pathBuilder.position(currentNode.asPosition().getPosition());
      } else {
        pathBuilder = pathBuilder.name(currentNode.asName().getName());
      }

      currentNode = currentNode.getChild().orElse(null);
    }

    return pathBuilder.build();
  }

  /**
   * Returns the int value nth path segment assuming it is a position segment.
   * @param path the path to parse
   * @param n index of the path segment
   * @return int value of the position at index n
   */
  protected int getPathSegmentAsPosition(ExpressionPath path, int n) {
    Optional<ExpressionPath.PathSegment> pathSegment = Optional.of(path.getRoot());

    for (int i = 0; i < n && pathSegment.isPresent(); i++) {
      pathSegment = pathSegment.get().getChild();
    }

    if (pathSegment.isPresent() && pathSegment.get().isPosition()) {
      return pathSegment.get().asPosition().getPosition();
    } else {
      throw new UnsupportedOperationException("Invalid expression path");
    }
  }

  /**
   * Removes a value from a field or subvalue of the field.
   * @param path the path of the node to remove
   */
  protected abstract void remove(ExpressionPath path);

  /**
   * Checks if a node is a list that supports remove and append.
   * @param path the path of the node to check
   * @return if the node is a list
   */
  protected abstract boolean fieldIsList(ExpressionPath path);

  /**
   * Adds a list of values to a node that is a list.
   * @param path the path of the node to append to
   * @param valuesToAdd list of values to add to the node
   */
  protected abstract void appendToList(ExpressionPath path, List<Entity> valuesToAdd);

  /**
   * Updates the value of a node.
   * @param path the path of the node to update
   * @param newValue the new value for the node
   */
  protected abstract void set(ExpressionPath path, Entity newValue);

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
