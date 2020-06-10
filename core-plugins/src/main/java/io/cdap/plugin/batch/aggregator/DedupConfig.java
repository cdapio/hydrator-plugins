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
import io.cdap.plugin.batch.aggregator.function.First;
import io.cdap.plugin.batch.aggregator.function.Last;
import io.cdap.plugin.batch.aggregator.function.MaxSelection;
import io.cdap.plugin.batch.aggregator.function.MinSelection;
import io.cdap.plugin.batch.aggregator.function.SelectionFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Deduplicate Plugin Configuration.
 */
public class DedupConfig extends AggregatorConfig {

  @Description("Optional comma-separated list of fields on which to perform the deduplication. If none given, each " +
    "record will be considered as a whole for deduplication. For example, if the input contains records with fields " +
    "'fname', 'lname', 'item', and 'cost' and we want to deduplicate the records by name, then this property should " +
    "be set to 'fname,lname'.")
  @Nullable
  @Macro
  private String uniqueFields;

  @Description("Optional property that can be set to predictably choose one or more records from the set of records " +
    "that needs to be deduplicated. This property takes in a field name and the logical operation that needs to be " +
    "performed on that field on the set of records. The syntax is 'field:function'. For example, if we want to " +
    "choose the record with maximum cost for the records with schema 'fname', 'lname', 'item', 'cost', then this " +
    "field should be set as 'cost:max'. Supported functions are first, last, max, and min. Note that only one pair " +
    "of field and function is allowed. If this property is not set, one random record will be chosen from the " +
    "group of 'duplicate' records.")
  @Nullable
  @Macro
  private String filterOperation;

  public DedupConfig() {
    this.uniqueFields = "";
    this.filterOperation = "";
  }

  @VisibleForTesting
  DedupConfig(String uniqueFields, String filterOperation) {
    this.uniqueFields = uniqueFields;
    this.filterOperation = filterOperation;
  }

  List<String> getUniqueFields() {
    List<String> uniqueFieldList = new ArrayList<>();
    if (!Strings.isNullOrEmpty(uniqueFields)) {
      for (String field : Splitter.on(',').trimResults().split(uniqueFields)) {
        uniqueFieldList.add(field);
      }
    }
    return uniqueFieldList;
  }

  @Nullable
  DedupFunctionInfo getFilter() {
    if (Strings.isNullOrEmpty(filterOperation)) {
      return null;
    }

    List<String> filterParts = new ArrayList<>();
    for (String filterPart : Splitter.on(':').trimResults().split(filterOperation)) {
      filterParts.add(filterPart);
    }

    if (filterParts.size() != 2) {
      throw new IllegalArgumentException(String.format("Invalid filter operation. It should be of format " +
                                                         "'fieldName:functionName'. But got : %s", filterOperation));
    }

    Function function;
    String fieldName = filterParts.get(0);
    String functionStr = filterParts.get(1);
    try {
      function = Function.valueOf(functionStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid function '%s'. Must be one of %s.",
                                                       functionStr, Joiner.on(',').join(Function.values())));
    }
    return new DedupFunctionInfo(fieldName, function);
  }

  static class DedupFunctionInfo {
    private final String field;
    private final Function function;

    DedupFunctionInfo(String field, Function function) {
      this.field = field;
      this.function = function;
    }

    public String getField() {
      return field;
    }

    public Function getFunction() {
      return function;
    }

    public SelectionFunction getSelectionFunction(Schema fieldSchema) {
      switch (function) {
        case FIRST:
          return new First(field, fieldSchema);
        case LAST:
          return new Last(field, fieldSchema);
        case MAX:
          return new MaxSelection(field, fieldSchema);
        case MIN:
          return new MinSelection(field, fieldSchema);
      }
      throw new IllegalArgumentException(String.format(
        "The function '%s' provided is not supported. It must be one of %s.",
        function, Joiner.on(',').join(Function.values())));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DedupFunctionInfo that = (DedupFunctionInfo) o;
      return Objects.equals(field, that.field) && Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, function);
    }

    @Override
    public String toString() {
      return "DedupFunctionInfo{" +
        "field='" + field + '\'' +
        ", function=" + function +
        '}';
    }
  }

  enum Function {
    FIRST,
    LAST,
    MIN,
    MAX
  }
}
