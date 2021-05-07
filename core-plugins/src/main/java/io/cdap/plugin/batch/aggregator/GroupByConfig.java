/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch.aggregator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import io.cdap.plugin.batch.aggregator.function.AnyIf;
import io.cdap.plugin.batch.aggregator.function.Avg;
import io.cdap.plugin.batch.aggregator.function.AvgIf;
import io.cdap.plugin.batch.aggregator.function.CollectList;
import io.cdap.plugin.batch.aggregator.function.CollectListIf;
import io.cdap.plugin.batch.aggregator.function.CollectSet;
import io.cdap.plugin.batch.aggregator.function.CollectSetIf;
import io.cdap.plugin.batch.aggregator.function.Concat;
import io.cdap.plugin.batch.aggregator.function.ConcatDistinct;
import io.cdap.plugin.batch.aggregator.function.ConcatDistinctIf;
import io.cdap.plugin.batch.aggregator.function.ConcatIf;
import io.cdap.plugin.batch.aggregator.function.CorrectedSumOfSquares;
import io.cdap.plugin.batch.aggregator.function.CorrectedSumOfSquaresIf;
import io.cdap.plugin.batch.aggregator.function.Count;
import io.cdap.plugin.batch.aggregator.function.CountAll;
import io.cdap.plugin.batch.aggregator.function.CountDistinct;
import io.cdap.plugin.batch.aggregator.function.CountDistinctIf;
import io.cdap.plugin.batch.aggregator.function.CountIf;
import io.cdap.plugin.batch.aggregator.function.CountNulls;
import io.cdap.plugin.batch.aggregator.function.First;
import io.cdap.plugin.batch.aggregator.function.JexlCondition;
import io.cdap.plugin.batch.aggregator.function.Last;
import io.cdap.plugin.batch.aggregator.function.LogicalAnd;
import io.cdap.plugin.batch.aggregator.function.LogicalAndIf;
import io.cdap.plugin.batch.aggregator.function.LogicalOr;
import io.cdap.plugin.batch.aggregator.function.LogicalOrIf;
import io.cdap.plugin.batch.aggregator.function.LongestString;
import io.cdap.plugin.batch.aggregator.function.LongestStringIf;
import io.cdap.plugin.batch.aggregator.function.Max;
import io.cdap.plugin.batch.aggregator.function.MaxIf;
import io.cdap.plugin.batch.aggregator.function.Min;
import io.cdap.plugin.batch.aggregator.function.MinIf;
import io.cdap.plugin.batch.aggregator.function.ShortestString;
import io.cdap.plugin.batch.aggregator.function.ShortestStringIf;
import io.cdap.plugin.batch.aggregator.function.Stddev;
import io.cdap.plugin.batch.aggregator.function.StddevIf;
import io.cdap.plugin.batch.aggregator.function.Sum;
import io.cdap.plugin.batch.aggregator.function.SumIf;
import io.cdap.plugin.batch.aggregator.function.SumOfSquares;
import io.cdap.plugin.batch.aggregator.function.SumOfSquaresIf;
import io.cdap.plugin.batch.aggregator.function.Variance;
import io.cdap.plugin.batch.aggregator.function.VarianceIf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Config for group by types of plugins.
 */
public class GroupByConfig extends AggregatorConfig {

  @Macro
  @Description("Aggregates to compute on grouped records. " +
    "Supported aggregate functions are count, count(*), sum, avg, min, max, first, last. " +
    "A function must specify the field it should be applied on, as well as the name it should be called. " +
    "Aggregates are specified using syntax: \"name:function(field)[, other aggregates]\"." +
    "For example, 'avgPrice:avg(price),cheapest:min(price)' will calculate two aggregates. " +
    "The first will create a field called 'avgPrice' that is the average of all 'price' fields in the group. " +
    "The second will create a field called 'cheapest' that contains the minimum 'price' field in the group")
  private final String aggregates;

  @Macro
  @Description("Comma separated list of record fields to group by. " +
    "All records with the same value for all these fields will be grouped together. " +
    "The records output by this aggregator will contain all the group by fields and aggregate fields. " +
    "For example, if grouping by the 'user' field and calculating a count aggregate called 'numActions', " +
    "output records will have a 'user' field and 'numActions' field.")
  private final String groupByFields;

  public GroupByConfig() {
    this.groupByFields = "";
    this.aggregates = "";
  }

  @VisibleForTesting
  GroupByConfig(String groupByFields, String aggregates) {
    this.groupByFields = groupByFields;
    this.aggregates = aggregates;
  }

  /**
   * @return the fields to group by. Returns an empty list if groupByFields contains a macro. Otherwise, the list
   * returned can never be empty.
   */
  List<String> getGroupByFields() {
    List<String> fields = new ArrayList<>();
    if (containsMacro("groupByFields")) {
      return fields;
    }
    for (String field : Splitter.on(',').trimResults().split(groupByFields)) {
      fields.add(field);
    }
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("The 'groupByFields' property must be set.");
    }
    return fields;
  }

  /**
   * @return the aggregates to perform. Returns an empty list if aggregates contains a macro. Otherwise, the list
   * returned can never be empty.
   */
  List<FunctionInfo> getAggregates() {
    List<FunctionInfo> functionInfos = new ArrayList<>();
    if (containsMacro("aggregates")) {
      return functionInfos;
    }
    Set<String> aggregateNames = new HashSet<>();
    for (String aggregate : Splitter.on(',').trimResults().split(aggregates)) {
      int colonIdx = aggregate.indexOf(':');
      if (colonIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find ':' separating aggregate name from its function in '%s'.", aggregate));
      }
      String name = aggregate.substring(0, colonIdx).trim();
      if (!aggregateNames.add(name)) {
        throw new IllegalArgumentException(String.format(
          "Cannot create multiple aggregate functions with the same name '%s'.", name));
      }

      String functionAndField = aggregate.substring(colonIdx + 1).trim();
      int leftParanIdx = functionAndField.indexOf('(');
      if (leftParanIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find '(' in function '%s'. Functions must be specified as function(field).",
          functionAndField));
      }
      String functionStr = functionAndField.substring(0, leftParanIdx).trim();
      Function function;
      try {
        function = Function.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. Must be one of %s.", functionStr, Joiner.on(',').join(Function.values())));
      }

      if (!functionAndField.endsWith(")")) {
        throw new IllegalArgumentException(String.format(
          "Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
          functionAndField));
      }
      int conditionIndex = functionAndField.toLowerCase().indexOf("condition(");
      // check if condition involved extract substring up to condition otherwise extract up to length of string
      int fieldEndIndex = (conditionIndex == -1) ? functionAndField.length() - 1 : conditionIndex - 2;
      String field = functionAndField.substring(leftParanIdx + 1, fieldEndIndex).trim();
      if (field.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. A field must be given as an argument.", functionAndField));
      }
      if (conditionIndex == -1 && function.isConditional()) {
        throw new IllegalArgumentException("Missing 'condition' property for conditional function.");
      }
      String functionCondition = null;
      if (conditionIndex != -1) {
        // extract condition from the string
        // example: exampleField:avgIf(field):condition(department.equals('d1'))
        // before the substring functionCondition is null, but after the substring it will be like:
        // department.equals('d1')
        functionCondition = functionAndField.substring(conditionIndex + 10, functionAndField.length() - 1);
        if (Strings.isNullOrEmpty(functionCondition)) {
          throw new IllegalArgumentException("The 'condition' property is missing arguments.");
        }
        functionCondition = functionCondition.trim();
      }
      functionInfos.add(new FunctionInfo(name, field, function, functionCondition));
    }

    if (functionInfos.isEmpty()) {
      throw new IllegalArgumentException("The 'aggregates' property must be set.");
    }
    return functionInfos;
  }

  /**
   * Class to hold information for an aggregate function.
   */
  static class FunctionInfo {
    private final String name;
    private final String field;
    private final Function function;
    private final String condition;

    FunctionInfo(String name, String field, Function function, String condition) {
      this.name = name;
      this.field = field;
      this.function = function;
      this.condition = condition;
    }

    FunctionInfo(String name, String field, Function function) {
      this.name = name;
      this.field = field;
      this.function = function;
      this.condition = null;
    }

    public String getName() {
      return name;
    }

    public String getField() {
      return field;
    }

    public Function getFunction() {
      return function;
    }

    public String getCondition() {
      return condition;
    }

    public AggregateFunction getAggregateFunction(Schema fieldSchema) {
      switch (function) {
        case COUNT:
          if ("*".equals(field)) {
            return new CountAll();
          }
          return new Count(field);
        case COUNTDISTINCT:
          return new CountDistinct(field);
        case SUM:
          return new Sum(field, fieldSchema);
        case AVG:
          return new Avg(field, fieldSchema);
        case MIN:
          return new Min(field, fieldSchema);
        case MAX:
          return new Max(field, fieldSchema);
        case FIRST:
          return new First(field, fieldSchema);
        case LAST:
          return new Last(field, fieldSchema);
        case STDDEV:
          return new Stddev(field, fieldSchema);
        case VARIANCE:
          return new Variance(field, fieldSchema);
        case COLLECTLIST:
          return new CollectList(field, fieldSchema);
        case COLLECTSET:
          return new CollectSet(field, fieldSchema);
        case LONGESTSTRING:
          return new LongestString(field, fieldSchema);
        case SHORTESTSTRING:
          return new ShortestString(field, fieldSchema);
        case COUNTNULLS:
          return new CountNulls(field);
        case CONCAT:
          return new Concat(field, fieldSchema);
        case CONCATDISTINCT:
          return new ConcatDistinct(field, fieldSchema);
        case LOGICALAND:
          return new LogicalAnd(field);
        case LOGICALOR:
          return new LogicalOr(field);
        case CORRECTEDSUMOFSQUARES:
          return new CorrectedSumOfSquares(field, fieldSchema);
        case SUMOFSQUARES:
          return new SumOfSquares(field, fieldSchema);
        case COUNTIF:
          return new CountIf(field, JexlCondition.of(condition));
        case COUNTDISTINCTIF:
          return new CountDistinctIf(field, JexlCondition.of(condition));
        case SUMIF:
          return new SumIf(field, fieldSchema, JexlCondition.of(condition));
        case AVGIF:
          return new AvgIf(field, fieldSchema, JexlCondition.of(condition));
        case MINIF:
          return new MinIf(field, fieldSchema, JexlCondition.of(condition));
        case MAXIF:
          return new MaxIf(field, fieldSchema, JexlCondition.of(condition));
        case STDDEVIF:
          return new StddevIf(field, fieldSchema, JexlCondition.of(condition));
        case VARIANCEIF:
          return new VarianceIf(field, fieldSchema, JexlCondition.of(condition));
        case COLLECTLISTIF:
          return new CollectListIf(field, fieldSchema, JexlCondition.of(condition));
        case COLLECTSETIF:
          return new CollectSetIf(field, fieldSchema, JexlCondition.of(condition));
        case LONGESTSTRINGIF:
          return new LongestStringIf(field, fieldSchema, JexlCondition.of(condition));
        case SHORTESTSTRINGIF:
          return new ShortestStringIf(field, fieldSchema, JexlCondition.of(condition));
        case CONCATIF:
          return new ConcatIf(field, fieldSchema, JexlCondition.of(condition));
        case CONCATDISTINCTIF:
          return new ConcatDistinctIf(field, fieldSchema, JexlCondition.of(condition));
        case LOGICALANDIF:
          return new LogicalAndIf(field, JexlCondition.of(condition));
        case LOGICALORIF:
          return new LogicalOrIf(field, JexlCondition.of(condition));
        case CORRECTEDSUMOFSQUARESIF:
          return new CorrectedSumOfSquaresIf(field, fieldSchema, JexlCondition.of(condition));
        case SUMOFSQUARESIF:
          return new SumOfSquaresIf(field, fieldSchema, JexlCondition.of(condition));
        case ANYIF:
          return new AnyIf(field, fieldSchema, JexlCondition.of(condition));
      }
      // should never happen
      throw new IllegalStateException("Unknown function type " + function);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FunctionInfo that = (FunctionInfo) o;

      return Objects.equals(name, that.name) &&
        Objects.equals(field, that.field) &&
        Objects.equals(function, that.function) &&
        Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, field, function, condition);
    }

    @Override
    public String toString() {
      return "FunctionInfo{" +
        "name='" + name + '\'' +
        ", field='" + field + '\'' +
        ", function=" + function + '\'' +
        ", condition=" + condition +
        '}';
    }
  }

  enum FunctionType {
    NONE,
    CONDITIONAL
  }

  enum Function {
    COUNT(FunctionType.NONE),
    COUNTDISTINCT(FunctionType.NONE),
    SUM(FunctionType.NONE),
    AVG(FunctionType.NONE),
    MIN(FunctionType.NONE),
    MAX(FunctionType.NONE),
    FIRST(FunctionType.NONE),
    LAST(FunctionType.NONE),
    STDDEV(FunctionType.NONE),
    VARIANCE(FunctionType.NONE),
    COLLECTLIST(FunctionType.NONE),
    COLLECTSET(FunctionType.NONE),
    LONGESTSTRING(FunctionType.NONE),
    SHORTESTSTRING(FunctionType.NONE),
    COUNTNULLS(FunctionType.NONE),
    CONCAT(FunctionType.NONE),
    CONCATDISTINCT(FunctionType.NONE),
    LOGICALAND(FunctionType.NONE),
    LOGICALOR(FunctionType.NONE),
    CORRECTEDSUMOFSQUARES(FunctionType.NONE),
    SUMOFSQUARES(FunctionType.NONE),
    COUNTIF(FunctionType.CONDITIONAL),
    COUNTDISTINCTIF(FunctionType.CONDITIONAL),
    SUMIF(FunctionType.CONDITIONAL),
    AVGIF(FunctionType.CONDITIONAL),
    MINIF(FunctionType.CONDITIONAL),
    MAXIF(FunctionType.CONDITIONAL),
    STDDEVIF(FunctionType.CONDITIONAL),
    VARIANCEIF(FunctionType.CONDITIONAL),
    COLLECTLISTIF(FunctionType.CONDITIONAL),
    COLLECTSETIF(FunctionType.CONDITIONAL),
    LONGESTSTRINGIF(FunctionType.CONDITIONAL),
    SHORTESTSTRINGIF(FunctionType.CONDITIONAL),
    CONCATIF(FunctionType.CONDITIONAL),
    CONCATDISTINCTIF(FunctionType.CONDITIONAL),
    LOGICALANDIF(FunctionType.CONDITIONAL),
    LOGICALORIF(FunctionType.CONDITIONAL),
    CORRECTEDSUMOFSQUARESIF(FunctionType.CONDITIONAL),
    SUMOFSQUARESIF(FunctionType.CONDITIONAL),
    ANYIF(FunctionType.CONDITIONAL);

    private final FunctionType type;

    Function(final FunctionType type) {
      this.type = type;
    }

    public boolean isConditional() {
      return this.type == FunctionType.CONDITIONAL;
    }
  }
}
