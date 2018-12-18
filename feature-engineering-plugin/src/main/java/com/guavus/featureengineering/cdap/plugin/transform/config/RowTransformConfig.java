/*
 * Copyright © 2018 Cask Data, Inc.
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
package com.guavus.featureengineering.cdap.plugin.transform.config;

import co.cask.cdap.api.data.schema.Schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.guavus.featureengineering.cdap.plugin.transform.function.Day;
import com.guavus.featureengineering.cdap.plugin.transform.function.Month;
import com.guavus.featureengineering.cdap.plugin.transform.function.NumCharacters;
import com.guavus.featureengineering.cdap.plugin.transform.function.NumWords;
import com.guavus.featureengineering.cdap.plugin.transform.function.PlusOneLog;
import com.guavus.featureengineering.cdap.plugin.transform.function.TimeDiffInMin;
import com.guavus.featureengineering.cdap.plugin.transform.function.TransformFunction;
import com.guavus.featureengineering.cdap.plugin.transform.function.WeekDay;
import com.guavus.featureengineering.cdap.plugin.transform.function.Year;

/**
 * @author bhupesh.goel
 *
 */
public class RowTransformConfig extends RowTransformConfigBase {

    /**
     * 
     */
    private static final long serialVersionUID = 8812797132453825429L;

    /**
     * @param primitives
     */
    @VisibleForTesting
    public RowTransformConfig(String primitives) {
        super(primitives);
    }

    /**
     * 
     */
    public RowTransformConfig() {
        super();
    }

    @Override
    protected String getValidTransformFunctionName(String functionName) {
        Function function;
        try {
            function = Function.valueOf(functionName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Invalid function '%s'. Must be one of %s.", functionName,
                    Joiner.on(',').join(Function.values())));
        }
        return function.name();
    }

    @Override
    protected FunctionInfo createFunctionInfoInstance(String name, String[] field, String functionName) {
        return new NormalFunctionInfo(name, field, functionName);
    }

    private static class NormalFunctionInfo extends FunctionInfo {

        NormalFunctionInfo(String name, String[] field, String function) {
            super(name, field, function);
        }

        @Override
        public TransformFunction getTransformFunction(Schema[] fieldSchema) {
            Function functionEnum = Function.valueOf(function);
            switch (functionEnum) {
            case DAY:
                return new Day(field[0], fieldSchema[0]);
            case YEAR:
                return new Year(field[0], fieldSchema[0]);
            case MONTH:
                return new Month(field[0], fieldSchema[0]);
            case WEEKDAY:
                return new WeekDay(field[0], fieldSchema[0]);
            case NUMWORDS:
                return new NumWords(field[0], fieldSchema[0]);
            case NUMCHARACTERS:
                return new NumCharacters(field[0], fieldSchema[0]);
            case PLUSONELOG:
                return new PlusOneLog(field[0], fieldSchema[0]);
            case TIMEDIFFINMIN:
                return new TimeDiffInMin(field, fieldSchema);
            }
            // should never happen
            throw new IllegalStateException("Unknown function type " + function);
        }

    }

    private enum Function {
        DAY, YEAR, MONTH, WEEKDAY, NUMWORDS, NUMCHARACTERS, PLUSONELOG, TIMEDIFFINMIN
    }
}
