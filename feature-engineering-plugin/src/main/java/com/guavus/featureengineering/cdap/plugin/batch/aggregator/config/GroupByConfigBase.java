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

package com.guavus.featureengineering.cdap.plugin.batch.aggregator.config;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.aggregator.AggregatorConfig;
import co.cask.hydrator.plugin.batch.aggregator.function.AggregateFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Config for group by types of plugins.
 */
public abstract class GroupByConfigBase extends AggregatorConfig {

    /**
     * 
     */
    private static final long serialVersionUID = 8472132896718615189L;

    @Description("Aggregates to compute on grouped records. "
            + "Supported aggregate functions are count, count(*), sum, avg, min, max, first, last. "
            + "A function must specify the field it should be applied on, as well as the name it should be called. "
            + "Aggregates are specified using syntax: \"name:function(field)[, other aggregates]\"."
            + "For example, 'avgPrice:avg(price),cheapest:min(price)' will calculate two aggregates. "
            + "The first will create a field called 'avgPrice' that is the average of all 'price' fields in the group."
            + "The second will create a field called 'cheapest' that contains the minimum 'price' field in the group")
    private final String aggregates;

    @Description("Comma separated list of record fields to group by. "
            + "All records with the same value for all these fields will be grouped together. "
            + "The records output by this aggregator will contain all the group by fields and aggregate fields. "
            + "For example, if grouping by the 'user' field and calculating a count aggregate called 'numActions', "
            + "output records will have a 'user' field and 'numActions' field.")
    private final String groupByFields;

    @Description("Place to specify all possible values for categorical columns.")
    @Nullable
    protected final String categoricalDictionary;

    public GroupByConfigBase() {
        this.groupByFields = "";
        this.aggregates = "";
        this.categoricalDictionary = "";
    }

    @VisibleForTesting
    GroupByConfigBase(String groupByFields, String aggregates, String categoricalDictionary) {
        this.groupByFields = groupByFields;
        this.aggregates = aggregates;
        this.categoricalDictionary = categoricalDictionary;
    }

    public List<String> getGroupByFields() {
        List<String> fields = new ArrayList<>();
        for (String field : Splitter.on(',').trimResults().split(groupByFields)) {
            fields.add(field);
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("The 'groupByFields' property must be set.");
        }
        return fields;
    }

    public List<FunctionInfo> getAggregates() {
        List<FunctionInfo> functionInfos = new ArrayList<>();
        Set<String> aggregateNames = new HashSet<>();
        for (String aggregate : Splitter.on(',').trimResults().split(aggregates)) {
            int colonIdx = aggregate.indexOf(':');
            if (colonIdx < 0) {
                throw new IllegalArgumentException(String
                        .format("Could not find ':' separating aggregate name from its function in '%s'.", aggregate));
            }
            String name = aggregate.substring(0, colonIdx).trim();
            if (!aggregateNames.add(name)) {
                throw new IllegalArgumentException(
                        String.format("Cannot create multiple aggregate functions with the same name '%s'.", name));
            }

            String functionAndField = aggregate.substring(colonIdx + 1).trim();
            int leftParanIdx = functionAndField.indexOf('(');
            if (leftParanIdx < 0) {
                throw new IllegalArgumentException(String.format(
                        "Could not find '(' in function '%s'. Functions must be specified as function(field).",
                        functionAndField));
            }
            String functionStr = functionAndField.substring(0, leftParanIdx).trim();

            if (!functionAndField.endsWith(")")) {
                throw new IllegalArgumentException(String.format(
                        "Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
                        functionAndField));
            }
            String[] field = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim()
                    .split("\\s+");
            if (field.length == 0) {
                throw new IllegalArgumentException(String
                        .format("Invalid function '%s'. A field must be given as an argument.", functionAndField));
            }
            String functionName = getValidAggregateFunctionName(functionStr);
            functionInfos.add(createFunctionInfoInstance(name, field, functionName));
        }

        if (functionInfos.isEmpty()) {
            throw new IllegalArgumentException("The 'aggregates' property must be set.");
        }
        return functionInfos;
    }

    protected abstract String getValidAggregateFunctionName(final String functionName);

    protected abstract FunctionInfo createFunctionInfoInstance(final String name, final String[] field,
            final String functionName);

    public abstract Map<String, List<String>> getCategoricalDictionaryMap();

    /**
     * Class to hold information for an aggregate function.
     */
    public abstract static class FunctionInfo {
        protected final String name;
        protected final String[] field;
        protected final String function;

        FunctionInfo(String name, String[] field, String function) {
            this.name = name;
            this.field = field;
            this.function = function;
        }

        public String getName() {
            return name;
        }

        public String[] getField() {
            return field;
        }

        public String getFunction() {
            return function;
        }

        public abstract AggregateFunction getAggregateFunction(Schema[] fieldSchema);

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FunctionInfo that = (FunctionInfo) o;

            return Objects.equals(name, that.name) && Objects.equals(field, that.field)
                    && Objects.equals(function, that.function);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, field, function);
        }

        @Override
        public String toString() {
            return "FunctionInfo{" + "name='" + name + '\'' + ", field='" + field + '\'' + ", function=" + function
                    + '}';
        }
    }

}
