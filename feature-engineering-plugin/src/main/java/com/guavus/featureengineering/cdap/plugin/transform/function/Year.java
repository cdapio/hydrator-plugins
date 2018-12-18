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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Method to get day of week from timestamp.
 * 
 * @author bhupesh.goel
 *
 */
public class Year implements TransformFunction<Integer> {

    private final String fieldName;
    private final Schema outputSchema;

    /**
     * 
     */
    public Year(String fieldName, Schema fieldSchema) {
        this.fieldName = fieldName;
        this.outputSchema = Schema.of(Schema.Type.INT);
    }

    @Override
    public Integer applyFunction(StructuredRecord record) {
        Object val = record.get(fieldName);
        if (val == null) {
            return 0;
        }
        String dateInString = val.toString();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
        DateTime dt = formatter.parseDateTime(dateInString);
        return dt.getYear();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
