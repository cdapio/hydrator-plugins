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

import java.util.HashSet;
import java.util.Set;

/**
 * @author bhupesh.goel
 *
 */
public class NUniq implements AggregateFunction<Integer> {

    private final String fieldName;
    private final Schema outputSchema;
    private Set<String> uniqueSet;

    public NUniq(String fieldName, Schema fieldSchema) {
        this.fieldName = fieldName;
        boolean isNullable = fieldSchema.isNullable();
        outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.INT)) : Schema.of(Schema.Type.INT);
    }

    @Override
    public void beginFunction() {
        uniqueSet = new HashSet<String>();
    }

    @Override
    public void operateOn(StructuredRecord record) {
        Object val = record.get(fieldName);
        if (val == null) {
            return;
        }
        String input = val.toString();
        uniqueSet.add(input.toLowerCase());
    }

    @Override
    public Integer getAggregate() {
        return uniqueSet.size();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
