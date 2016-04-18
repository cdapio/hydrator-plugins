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
import co.cask.hydrator.plugin.batch.aggregator.function.Sum;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;

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

  List<GroupByFunctionInfo> getAggregates() {
    List<FunctionInfo> functionInfos = parseAggregation(aggregates);
    if (functionInfos.isEmpty()) {
      throw new IllegalArgumentException("The 'aggregates' property must be set.");
    }

    List<GroupByFunctionInfo> groupByFunctionInfos = new ArrayList<>();
    for (FunctionInfo functionInfo : functionInfos) {
      groupByFunctionInfos.add(new GroupByFunctionInfo(functionInfo.getName(), functionInfo.getField(),
                                                       functionInfo.getFunction()));
    }
    return groupByFunctionInfos;
  }

  /**
   * Class to hold information for an aggregate function.
   */
  static class GroupByFunctionInfo extends FunctionInfo {

    public GroupByFunctionInfo(String name, String field, Function function) {
      super(name, field, function);
    }

    public AggregateFunction getAggregateFunction(Schema fieldSchema) {
      switch (getFunction()) {
        case COUNT:
          if ("*".equals(getField())) {
            return new CountAll();
          }
          return new Count(getField());
        case SUM:
          return new Sum(getField(), fieldSchema);
        case AVG:
          return new Avg(getField(), fieldSchema);
        case MIN:
          return new Min(getField(), fieldSchema);
        case MAX:
          return new Max(getField(), fieldSchema);
        case FIRST:
          return new First(getField(), fieldSchema);
        case LAST:
          return new Last(getField(), fieldSchema);
      }
      // should never happen
      throw new IllegalStateException("Unknown function type " + getFunction());
    }
  }
}
