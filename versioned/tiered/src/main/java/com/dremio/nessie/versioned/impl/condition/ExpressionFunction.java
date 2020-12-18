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
package com.dremio.nessie.versioned.impl.condition;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;

public class ExpressionFunction implements Value {

  enum FunctionName {
    LIST_APPEND("list_append", 2),
    IF_NOT_EXISTS("if_not_exists", 2),
    EQUALS("="),
    // Not yet implemented below here.
    //  ATTRIBUTE_EXISTS("attribute_exists", 1),
    ATTRIBUTE_NOT_EXISTS("attribute_not_exists", 1),
    SIZE("size", 1),
    //  ATTRIBUTE_TYPE("attribute_type", 1),
    //  BEGINS_WITH("begins_with", 2),
    //  CONTAINS("contains", 2),
    //    GT(">"),
    //    LT("<"),
    //    LTE("<="),
    //    GTE(">=")
    ;

    String protocolName;
    int argCount;
    boolean binaryExpression;

    FunctionName(String text, int argCount) {
      this(text, argCount, false);
    }

    FunctionName(String text, int argCount, boolean binaryExpression) {
      this.protocolName = text;
      this.argCount = argCount;
      this.binaryExpression = binaryExpression;
    }

    FunctionName(String text) {
      this(text, 2, true);
    }
  }

  private static final String OPERAND = "OPERAND";
  private static final String SIZE = "SIZE";

  private final FunctionName name;
  private final List<Value> arguments;

  ExpressionFunction(FunctionName name, ImmutableList<Value> arguments) {
    this.name = name;
    this.arguments = ImmutableList.copyOf(arguments);
    Preconditions.checkArgument(this.arguments.size() == name.argCount, "Unexpected argument count.");
  }

  public static ExpressionFunction size(ExpressionPath path) {
    return new ExpressionFunction(FunctionName.SIZE, ImmutableList.of(path));
  }

  public static ExpressionFunction appendToList(ExpressionPath initialList, Entity valueToAppend) {
    return new ExpressionFunction(FunctionName.LIST_APPEND, ImmutableList.of(initialList, Value.of(valueToAppend)));
  }

  public static ExpressionFunction attributeNotExists(ExpressionPath path) {
    return new ExpressionFunction(FunctionName.ATTRIBUTE_NOT_EXISTS, ImmutableList.of(path));
  }

  public static ExpressionFunction equals(ExpressionPath path, Entity value) {
    return new ExpressionFunction(FunctionName.EQUALS, ImmutableList.of(path, Value.of(value)));
  }

  public static ExpressionFunction equals(ExpressionFunction func, Entity value) {
    return new ExpressionFunction(FunctionName.EQUALS, ImmutableList.of(func, Value.of(value)));
  }


  public static ExpressionFunction ifNotExists(ExpressionPath path, Entity value) {
    return new ExpressionFunction(FunctionName.IF_NOT_EXISTS, ImmutableList.of(path, Value.of(value)));
  }

  /**
   * Return this function as a Dynamo expression string.
   * @return The expression string.
   */
  public String asString() {
    if (name.binaryExpression) {
      return String.format("%s %s %s", arguments.get(0).asString(), name.protocolName, arguments.get(1).asString());
    }
    return String.format("%s(%s)", name.protocolName, arguments.stream().map(v -> v.asString()).collect(Collectors.joining(", ")));
  }

  @Override
  public ExpressionFunction alias(AliasCollector c) {
    return new ExpressionFunction(name, arguments.stream().map(v -> v.alias(c)).collect(ImmutableList.toImmutableList()));
  }

  @Override
  public Type getType() {
    return Type.FUNCTION;
  }

  @Override
  public ExpressionFunction getFunction() {
    return this;
  }

  /**
   * Accept from the Value interface is not valid for ExpressionFunctions.
   * @param visitor the instance visiting.
   * @return Not valid for ExpressionFunctions.
   */
  @Override
  public Value acceptValue(ConditionAliasVisitor visitor) {
    throw new UnsupportedOperationException();
  }


  /**
   * This is part of the Visitor design pattern.
   * This method is called by visiting classes. In response their visit method is called back.
   * @param visitor the instance visiting.
   * @param <T> The class to which ExpressionFunction is converted
   * @return the converted class
   */
  public <T> T accept(ExpressionFunctionVisitor<T> visitor) {
    return visitor.visit(this, arguments, name);
  }

  /**
   * Creates a SIZE representation if the ExpressionFunction is a SIZE type.
   * @param visitor the instance visiting.
   * @param arguments the attributes of the ExpressionFunction.
   * @param <T> The class to which ExpressionFunction is converted
   * @return the converted class
   */
  public <T> T accept(ExpressionFunctionVisitor<T> visitor, List<Value> arguments) {
    ArrayListMultimap<String, String> operands = ArrayListMultimap.create();
    for (Value v : arguments) {
      operands.putAll(addToMap(v));
    }
    if (operands.containsKey(SIZE)) {
      return visitor.visit(this, operands.get(SIZE).get(0), operands.get(OPERAND).get(0));
    }
    return null;
  }

  /**
   * This is part of the Visitor design pattern.
   * This method is called by visiting classes. In response their visit method is called back.
   * @param visitor the instance visiting.
   * @return the aliased ExpressionFunction.
   */
  public ExpressionFunction acceptExpressionFunction(ConditionAliasVisitor visitor) {
    return visitor.visit(this, arguments, name);
  }

  private ArrayListMultimap<String, String> addToMap(Value value) {
    ArrayListMultimap<String, String> map = ArrayListMultimap.create();
    if (value instanceof ExpressionFunction && ((ExpressionFunction)value).name == FunctionName.SIZE) {
      map.put(SIZE, ((ExpressionFunction) value).arguments.get(0).asString());
    } else {
      map.put(OPERAND, value.asString());
    }
    return map;
  }

}
