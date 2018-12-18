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
package com.guavus.featureengineering.cdap.plugin.transform.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

/**
 * Method to take log of input value plus one.
 * 
 * @author bhupesh.goel
 *
 */
public class PlusOneLog implements TransformFunction<Double> {

    private final String fieldName;
    private final Schema outputSchema;

    public PlusOneLog(final String fieldName, final Schema fieldSchema) {
        this.fieldName = fieldName;
        boolean isNullable = fieldSchema.isNullable();
        this.outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))
                : Schema.of(Schema.Type.DOUBLE);
    }

    @Override
    public Double applyFunction(StructuredRecord record) {
        Object val = record.get(fieldName);
        if (val == null) {
            return 0.0;
        }
        return Math.log(((Number) val).doubleValue() + 1.0);
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public static void main(String args[]) {
        Integer t = 1;

        System.out.println(((Number) t).doubleValue());
    }
}
