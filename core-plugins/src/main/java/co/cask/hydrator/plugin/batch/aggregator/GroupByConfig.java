/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.aggregator.function.AggregateFunction;
import co.cask.hydrator.plugin.batch.aggregator.function.Avg;
import co.cask.hydrator.plugin.batch.aggregator.function.Count;
import co.cask.hydrator.plugin.batch.aggregator.function.CountAll;
import co.cask.hydrator.plugin.batch.aggregator.function.First;
import co.cask.hydrator.plugin.batch.aggregator.function.Last;
import co.cask.hydrator.plugin.batch.aggregator.function.Max;
import co.cask.hydrator.plugin.batch.aggregator.function.Min;
import co.cask.hydrator.plugin.batch.aggregator.function.Stddev;
import co.cask.hydrator.plugin.batch.aggregator.function.Sum;
import co.cask.hydrator.plugin.batch.aggregator.function.Variance;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Config for group by types of plugins.
 */
public class GroupByConfig extends AggregatorConfig {

  @Description("Aggregates to compute on grouped records. " +
    "Supported aggregate functions are count, count(*), sum, avg, min, max, first, last. " +
    "A function must specify the field it should be applied on, as well as the name it should be called. " +
    "Aggregates are specified using syntax: \"name:function(field)[, other aggregates]\"." +
    "For example, 'avgPrice:avg(price),cheapest:min(price)' will calculate two aggregates. " +
    "The first will create a field called 'avgPrice' that is the average of all 'price' fields in the group. " +
    "The second will create a field called 'cheapest' that contains the minimum 'price' field in the group")
  private final String aggregates;

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

  List<String> getGroupByFields() {
    List<String> fields = new ArrayList<>();
    for (String field : Splitter.on(',').trimResults().split(groupByFields)) {
      fields.add(field);
    }
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("The 'groupByFields' property must be set.");
    }
    return fields;
  }

  List<FunctionInfo> getAggregates() {
    List<FunctionInfo> functionInfos = new ArrayList<>();
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
      String field = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim();
      if (field.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. A field must be given as an argument.", functionAndField));
      }

      functionInfos.add(new FunctionInfo(name, field, function));
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

    public FunctionInfo(String name, String field, Function function) {
      this.name = name;
      this.field = field;
      this.function = function;
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

    public AggregateFunction getAggregateFunction(Schema fieldSchema) {
      switch (function) {
        case COUNT:
          if ("*".equals(field)) {
            return new CountAll();
          }
          return new Count(field);
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
        Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, field, function);
    }

    @Override
    public String toString() {
      return "FunctionInfo{" +
        "name='" + name + '\'' +
        ", field='" + field + '\'' +
        ", function=" + function +
        '}';
    }
  }

  enum Function {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    FIRST,
    LAST,
    STDDEV,
    VARIANCE
  }
}
