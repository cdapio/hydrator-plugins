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
package com.guavus.featureengineering.cdap.plugin.batch.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.PluginFunction;
import co.cask.cdap.api.annotation.PluginInput;
import co.cask.cdap.api.annotation.PluginOutput;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchAggregator;

import com.guavus.featureengineering.cdap.plugin.batch.aggregator.config.GroupByConfig;

import javax.ws.rs.Path;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupByAggregateFE")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group. "
        + "Supports avg, count, count(*), first, last, max, min, and sum as aggregate functions.")
@PluginInput(type = { "long:int:double:float", "*", "*", "*", "*", "*", "long:int:double:float",
        "long:int:double:float", "long:int:double:float", "string:int:long" })
@PluginOutput(
        type = { "double", "long", "same", "same", "same", "same", "double", "long:int:double:float", "double", "int" })
@PluginFunction(function = { "avg", "count", "first", "last", "min", "max", "stddev", "sum", "variance", "nuniq" })
public class GroupByAggregator extends GroupByAggregatorBase {
    private final GroupByConfig conf;

    /**
     * @param conf
     */
    public GroupByAggregator(GroupByConfig conf) {
        super(conf);
        this.conf = conf;
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getStaticOutputSchema(request.inputSchema, request.getGroupByFields(), request.getAggregates(), null,
                null, null);
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends GroupByConfig {
        /**
         * 
         */
        private static final long serialVersionUID = -6237550702080539086L;
        private Schema inputSchema;
    }
}
