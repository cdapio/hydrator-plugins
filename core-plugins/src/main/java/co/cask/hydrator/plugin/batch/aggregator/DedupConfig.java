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
import co.cask.hydrator.plugin.batch.aggregator.function.First;
import co.cask.hydrator.plugin.batch.aggregator.function.Last;
import co.cask.hydrator.plugin.batch.aggregator.function.RecordAggregateFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Plugin Configuration
 */
public class DedupConfig extends AggregatorConfig {

  @Description("Optional comma-separated list of fields to perform the distinct on. If none is given, each record " +
    "will be taken as is. Otherwise, only fields in this list will be considered.")
  @Nullable
  private String uniqueFields;

  @Description("Optional field that can be used to choose ")
  @Nullable
  private String filterField;

  @VisibleForTesting
  DedupConfig(String uniqueFields, String filterField) {
    this.uniqueFields = uniqueFields;
    this.filterField = filterField;
  }

  List<String> getUniqueFields() {
    List<String> uniqueFieldList = new ArrayList<>();
    for (String field : Splitter.on(',').trimResults().split(uniqueFields)) {
      uniqueFieldList.add(field);
    }
    return uniqueFieldList;
  }

  @Nullable
  RecordAggregationFunctionInfo getFilter() {
    if (filterField == null) {
      return null;
    }

    int colonIdx = filterField.indexOf(':');
    if (colonIdx < 0) {
      throw new IllegalArgumentException(String.format("Could not find ':' separating aggregate name from its" +
                                                         "function in '%s'", filterField));
    }

    String functionAndField = filterField.substring(colonIdx + 1).trim();
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

    return new RecordAggregationFunctionInfo(filterField, field, function);
  }

  static class RecordAggregationFunctionInfo {
    private final String name;
    private final String field;
    private final Function function;

    public RecordAggregationFunctionInfo(String name, String field, Function function) {
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

    public RecordAggregateFunction getAggregateFunction(Schema fieldSchema) {
      switch (function) {
        case FIRST:
          return new First(field, fieldSchema);
        case LAST:
          return new Last(field, fieldSchema);
      }
      throw new IllegalStateException("Unknown or Unsupported function type " + function);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RecordAggregationFunctionInfo that = (RecordAggregationFunctionInfo) o;

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
      return "RecordAggregationFunctionInfo{" +
        "name='" + name + '\'' +
        ", field='" + field + '\'' +
        ", function=" + function +
        '}';
    }
  }

  enum Function {
    FIRST,
    LAST
  }
}
