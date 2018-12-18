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

import com.guavus.featureengineering.cdap.plugin.batch.aggregator.config.GroupByConfigCategorical;

import javax.ws.rs.Path;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupByCategoricalAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group values. "
        + "Supports valuecount, indicatorcount, nuniq as aggregate functions.")
@PluginInput(type = { "string:int:long", "string:int:long", "string:int:long string:int:long" })
@PluginOutput(type = { "list<int>", "list<int>", "list<int>" })
@PluginFunction(function = { "valuecount", "indicatorcount", "catcrossproduct" })
public class GroupByCategoricalAggregator extends GroupByAggregatorBase {

    private final GroupByConfigCategorical conf;

    /**
     * @param conf
     */
    public GroupByCategoricalAggregator(GroupByConfigCategorical conf) {
        super(conf);
        this.conf = conf;
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getStaticOutputSchema(request.inputSchema, request.getGroupByFields(), request.getAggregates(),
                request.getCategoricalDictionaryMap(), null, null);
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends GroupByConfigCategorical {
        private Schema inputSchema;
    }
}
