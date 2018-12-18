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
package com.guavus.featureengineering.cdap.plugin.batch.aggregator.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.aggregator.function.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public class CatCrossProduct implements AggregateFunction<Map<String, Map<String, Integer>>> {

    private final String[] fieldName;
    private final Schema outputSchema;
    private Map<String, Map<String, Integer>> valueSpecificFrequencyCount;

    public CatCrossProduct(String[] fieldName, Schema[] fieldSchema) {
        this.fieldName = fieldName;
        boolean isNullable = false;
        for (Schema schema : fieldSchema) {
            if (schema.isNullable()) {
                isNullable = true;
                break;
            }
        }
        this.outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.INT)) : Schema.of(Schema.Type.INT);
    }

    @Override
    public void beginFunction() {
        this.valueSpecificFrequencyCount = new HashMap<>();
    }

    @Override
    public void operateOn(StructuredRecord record) {
        Object val1 = record.get(fieldName[0]);
        if (val1 == null) {
            return;
        }
        String input1 = val1.toString().toLowerCase();

        Object val2 = record.get(fieldName[1]);
        if (val2 == null) {
            return;
        }
        String input2 = val2.toString().toLowerCase();

        Map<String, Integer> frequencyCount = valueSpecificFrequencyCount.get(input2);
        if (frequencyCount == null) {
            frequencyCount = new HashMap<>();
            valueSpecificFrequencyCount.put(input2, frequencyCount);
        }
        if (!frequencyCount.containsKey(input1)) {
            frequencyCount.put(input1, 0);
        }
        frequencyCount.put(input1, 1 + frequencyCount.get(input1));
    }

    @Override
    public Map<String, Map<String, Integer>> getAggregate() {
        return valueSpecificFrequencyCount;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
