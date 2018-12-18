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

import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author bhupesh.goel
 *
 */
public class TimeDiffInMin implements TransformFunction<Integer> {

    private final String[] fieldName;
    private final Schema outputSchema;

    /**
     * 
     */
    public TimeDiffInMin(String[] fieldName, Schema[] fieldSchemas) {
        this.fieldName = fieldName;
        boolean isNullable = false;
        for (Schema schema : fieldSchemas) {
            if (schema.isNullable()) {
                isNullable = true;
                break;
            }
        }
        this.outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.INT)) : Schema.of(Schema.Type.INT);
    }

    @Override
    public Integer applyFunction(StructuredRecord record) {
        Object val1 = record.get(fieldName[0]);
        if (val1 == null) {
            return null;
        }
        Object val2 = record.get(fieldName[1]);
        if (val2 == null) {
            return null;
        }
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

        String dateInString1 = val1.toString();
        String dateInString2 = val2.toString();

        DateTime dt1 = formatter.parseDateTime(dateInString1);
        DateTime dt2 = formatter.parseDateTime(dateInString2);
        return Minutes.minutesBetween(dt1, dt2).getMinutes();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
