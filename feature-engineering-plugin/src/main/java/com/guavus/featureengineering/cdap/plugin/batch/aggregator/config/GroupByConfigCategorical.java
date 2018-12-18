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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.aggregator.function.AggregateFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.CatCrossProduct;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.IndicatorCount;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.ValueCount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Config for group by types of plugins.
 */
public class GroupByConfigCategorical extends GroupByConfig {

    /**
     * 
     */
    private static final long serialVersionUID = 3137515134995026694L;

    public GroupByConfigCategorical() {
        super();
    }

    @VisibleForTesting
    GroupByConfigCategorical(String groupByFields, String aggregates, String categoricalDictionary) {
        super(groupByFields, aggregates, categoricalDictionary);
    }

    @Override
    protected String getValidAggregateFunctionName(final String functionName) {
        String function;
        try {
            function = Function.valueOf(functionName.toUpperCase()).name();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Invalid function '%s'. Must be one of %s.", functionName,
                    Joiner.on(',').join(Function.values())));
        }
        return function;
    }

    @Override
    protected FunctionInfo createFunctionInfoInstance(final String name, final String[] field,
            final String functionName) {
        return new CategoricalFunctionInfo(name, field, functionName);
    }

    /**
     * @return the categoricalDictionary
     */
    @Override
    public Map<String, List<String>> getCategoricalDictionaryMap() {
        if (categoricalDictionary == null || categoricalDictionary.isEmpty()) {
            return null;
        }
        Map<String, List<String>> categoricalDictionaryMap = new HashMap<>();
        for (String field : Splitter.on(',').trimResults().split(categoricalDictionary)) {
            String tokens[] = field.split(":");
            List<String> categoricalDictionaryFields = new ArrayList<String>();
            categoricalDictionaryFields = Arrays.asList(tokens[1].trim().split(";"));
            categoricalDictionaryMap.put(tokens[0].trim().toLowerCase(), categoricalDictionaryFields);
        }
        return categoricalDictionaryMap;
    }

    /**
     * Class to hold information for an aggregate function.
     */
    public static class CategoricalFunctionInfo extends FunctionInfo {

        CategoricalFunctionInfo(String name, String[] field, String function) {
            super(name, field, function);
        }

        @Override
        public AggregateFunction getAggregateFunction(Schema[] fieldSchema) {
            Function functionEnum = Function.valueOf(function);
            switch (functionEnum) {
            case VALUECOUNT:
                return new ValueCount(field[0], fieldSchema[0]);
            case INDICATORCOUNT:
                return new IndicatorCount(field[0], fieldSchema[0]);
            case CATCROSSPRODUCT:
                return new CatCrossProduct(field, fieldSchema);
            }
            // should never happen
            throw new IllegalStateException("Unknown function type " + function);
        }

    }

    private enum Function {
        CATCROSSPRODUCT, VALUECOUNT, INDICATORCOUNT
    }
}
