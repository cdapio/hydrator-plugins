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
package com.guavus.featureengineering.cdap.plugin.transform.config;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.guavus.featureengineering.cdap.plugin.transform.function.EQ;
import com.guavus.featureengineering.cdap.plugin.transform.function.FilterFunction;
import com.guavus.featureengineering.cdap.plugin.transform.function.GT;
import com.guavus.featureengineering.cdap.plugin.transform.function.GTE;
import com.guavus.featureengineering.cdap.plugin.transform.function.LT;
import com.guavus.featureengineering.cdap.plugin.transform.function.LTE;
import com.guavus.featureengineering.cdap.plugin.transform.function.NEQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author bhupesh.goel
 *
 */
public class FilterTransformConfig extends PluginConfig {

    /**
     * 
     */
    private static final long serialVersionUID = 4077946383526119822L;

    @Description("Filter transform to filter on given records. "
            + "Supported filters are LessThan, greaterThan, LessThanEqual, greaterThanEqual, Equal. "
            + "A filter must be specified along with target column and value for filter.")
    private final String filters;

    @VisibleForTesting
    FilterTransformConfig(String filters) {
        this.filters = filters;
    }

    /**
     * 
     */
    public FilterTransformConfig() {
        this.filters = "";
    }

    public List<FilterInfo> getFilters() {
        List<FilterInfo> functionInfos = new ArrayList<>();
        for (String filter : Splitter.on(',').trimResults().split(filters)) {
            int colonIdx = filter.indexOf(':');
            if (colonIdx < 0) {
                throw new IllegalArgumentException(String
                        .format("Could not find ':' separating primitive name from its function in '%s'.", filter));
            }
            String value = filter.substring(0, colonIdx).trim();

            String functionAndField = filter.substring(colonIdx + 1).trim();
            int leftParanIdx = functionAndField.indexOf('(');
            if (leftParanIdx < 0) {
                throw new IllegalArgumentException(String.format(
                        "Could not find '(' in function '%s'. Functions must be specified as function(field).",
                        functionAndField));
            }
            String functionStr = functionAndField.substring(0, leftParanIdx).trim();
            Function function;
            try {
                function = Function.valueOf(functionStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("Invalid function '%s'. Must be one of %s.",
                        functionStr, Joiner.on(',').join(Function.values())));
            }

            if (!functionAndField.endsWith(")")) {
                throw new IllegalArgumentException(String.format(
                        "Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
                        functionAndField));
            }
            String field = functionAndField.substring(leftParanIdx + 1, functionAndField.length() - 1).trim();
            if (field.isEmpty()) {
                throw new IllegalArgumentException(String
                        .format("Invalid function '%s'. A field must be given as an argument.", functionAndField));
            }

            functionInfos.add(new FilterInfo(value, field, function));
        }

        if (functionInfos.isEmpty()) {
            throw new IllegalArgumentException("The 'primitive' property must be set.");
        }
        return functionInfos;
    }

    /**
     * Class to hold information for an primitive function.
     */
    public static class FilterInfo {
        private final String value;
        private final String field;
        private final Function function;

        FilterInfo(String value, String field, Function function) {
            this.value = value;
            this.field = field;
            this.function = function;
        }

        public String getValue() {
            return value;
        }

        public String getField() {
            return field;
        }

        public Function getFunction() {
            return function;
        }

        public FilterFunction getFilterFunction(Schema fieldSchema) {
            switch (function) {
            case LT:
                return new LT(field, value);
            case GT:
                return new GT(field, value);
            case LTE:
                return new LTE(field, value);
            case GTE:
                return new GTE(field, value);
            case EQ:
                return new EQ(field, value);
            case NEQ:
                return new NEQ(field, value);
            }
            // should never happen
            throw new IllegalStateException("Unknown function type " + function);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FilterInfo that = (FilterInfo) o;

            return Objects.equals(value, that.value) && Objects.equals(field, that.field)
                    && Objects.equals(function, that.function);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, field, function);
        }

        @Override
        public String toString() {
            return "FunctionInfo{" + "name='" + value + '\'' + ", field='" + field + '\'' + ", function=" + function
                    + '}';
        }
    }

    enum Function {
        LT, GT, LTE, GTE, EQ, NEQ
    }
}
