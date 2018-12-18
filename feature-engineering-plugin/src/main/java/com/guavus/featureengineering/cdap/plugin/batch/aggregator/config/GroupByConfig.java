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

import com.google.common.base.Joiner;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.CatCrossProduct;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.IndicatorCount;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.NUniq;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.function.ValueCount;

import java.util.List;
import java.util.Map;

/**
 * Config for group by types of plugins.
 */
public class GroupByConfig extends GroupByConfigBase {

    public GroupByConfig() {
        super();
    }

    public GroupByConfig(String groupByFields, String aggregates, String categoricalDictionary) {
        super(groupByFields, aggregates, categoricalDictionary);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 8472132896718615189L;

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
        return new NormalFunctionInfo(name, field, functionName);
    }

    @Override
    public Map<String, List<String>> getCategoricalDictionaryMap() {
        return null;
    }

    /**
     * Class to hold information for an aggregate function.
     */
    public static class NormalFunctionInfo extends FunctionInfo {

        NormalFunctionInfo(String name, String[] field, String function) {
            super(name, field, function);
        }

        @Override
        public AggregateFunction getAggregateFunction(Schema[] fieldSchema) {
            Function functionEnum = Function.valueOf(function);
            switch (functionEnum) {
            case COUNT:
                if ("*".equals(field[0])) {
                    return new CountAll();
                }
                return new Count(field[0]);
            case SUM:
                return new Sum(field[0], fieldSchema[0]);
            case AVG:
                return new Avg(field[0], fieldSchema[0]);
            case MIN:
                return new Min(field[0], fieldSchema[0]);
            case MAX:
                return new Max(field[0], fieldSchema[0]);
            case FIRST:
                return new First(field[0], fieldSchema[0]);
            case LAST:
                return new Last(field[0], fieldSchema[0]);
            case STDDEV:
                return new Stddev(field[0], fieldSchema[0]);
            case VARIANCE:
                return new Variance(field[0], fieldSchema[0]);
            case NUNIQ:
                return new NUniq(field[0], fieldSchema[0]);
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
        COUNT, SUM, AVG, MIN, MAX, FIRST, LAST, STDDEV, VARIANCE, NUNIQ, CATCROSSPRODUCT, VALUECOUNT, INDICATORCOUNT
    }
}
