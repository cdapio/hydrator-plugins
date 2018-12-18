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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author bhupesh.goel
 *
 */
public abstract class FilterFunction {

    public abstract Boolean applyFilter(StructuredRecord record);

    final int comparisonResult(Object inputField, String inputValue) {
        try {
            long longValue = Long.parseLong(inputValue);
            Long inputFieldValue = (Long) inputField;
            return inputFieldValue.compareTo(new Long(longValue));
        } catch (Exception e) {
            try {
                double doubleValue = Double.parseDouble(inputValue);
                Double inputFieldValue = (Double) inputField;
                return inputFieldValue.compareTo(new Double(doubleValue));
            } catch (Exception ex) {
                try {
                    boolean booleanValue = Boolean.parseBoolean(inputValue);
                    Boolean inputFieldValue = (Boolean) inputField;
                    return inputFieldValue.compareTo(new Boolean(booleanValue));
                } catch (Exception eex) {
                    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
                    try {
                        long inputTime = Long.parseLong(inputValue);
                        DateTime value = new DateTime(inputTime, DateTimeZone.UTC);
                        DateTime input = formatter.parseDateTime((String) inputField);
                        return input.compareTo(value);
                    } catch (Exception eeex) {
                        return (inputField.toString()).compareTo(inputValue);
                    }
                }
            }
        }
    }
}
