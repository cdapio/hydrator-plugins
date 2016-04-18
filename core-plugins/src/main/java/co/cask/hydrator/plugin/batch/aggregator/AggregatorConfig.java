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
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base aggregator config, since almost all aggregators should support setting the number of partitions.
 */
public class AggregatorConfig extends PluginConfig {
  @Nullable
  @Description("Number of partitions to use when aggregating. If not specified, the execution framework " +
    "will decide how many to use.")
  protected Integer numPartitions;

  public List<FunctionInfo> parseAggregation(String aggregates) {
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
    return functionInfos;
  }

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
    LAST
  }
}
