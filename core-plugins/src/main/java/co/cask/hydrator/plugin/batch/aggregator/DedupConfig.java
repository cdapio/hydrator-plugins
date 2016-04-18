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
import co.cask.hydrator.plugin.batch.aggregator.function.Max;
import co.cask.hydrator.plugin.batch.aggregator.function.Min;
import co.cask.hydrator.plugin.batch.aggregator.function.RecordAggregateFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;
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

  public DedupConfig() {
    this.uniqueFields = "";
    this.filterField = "";
  }

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
  DedupFunctionInfo getFilter() {
    if (filterField == null) {
      return null;
    }

    List<FunctionInfo> aggregates = parseAggregation(filterField);
    if (aggregates.size() != 1) {
      throw new IllegalArgumentException("Only one filter field is allowed!");
    }
    FunctionInfo aggregate = aggregates.get(0);
    return new DedupFunctionInfo(aggregate.getName(), aggregate.getField(), aggregate.getFunction());
  }

  static class DedupFunctionInfo extends FunctionInfo {

    public DedupFunctionInfo(String name, String field, Function function) {
      super(name, field, function);
    }

    public RecordAggregateFunction getAggregateFunction(Schema recordSchema) {
      Schema.Field recordField = recordSchema.getField(getField());
      Schema fieldSchema = recordField.getSchema();
      switch (getFunction()) {
        case MAX:
          return new Max(getField(), fieldSchema);
        case MIN:
          return new Min(getField(), fieldSchema);
        case FIRST:
          return new First(getField(), fieldSchema);
        case LAST:
          return new Last(getField(), fieldSchema);
      }
      throw new IllegalStateException("Unknown or Unsupported function type " + getFunction());
    }
  }
}
