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
package com.guavus.featureengineering.cdap.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.PluginFunction;
import co.cask.cdap.api.annotation.PluginInput;
import co.cask.cdap.api.annotation.PluginOutput;
import co.cask.cdap.api.data.schema.Schema;

import com.guavus.featureengineering.cdap.plugin.transform.config.RowTransformConfig;

import javax.ws.rs.Path;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = "transform")
@Name("RowTransform")
@Description("Executes transform primitives to add new columns in record.")
@PluginInput(type = { "datetime", "datetime", "datetime", "datetime", "string", "string", "double:int:long:float",
        "string string" })
@PluginOutput(type = { "int", "int", "int", "int", "int", "int", "double", "int" })
@PluginFunction(
        function = { "day", "year", "month", "weekday", "numwords", "numcharacters", "plusonelog", "timediffinmin" })
public class RowTransform extends RowTransformBase {

    private final RowTransformConfig conf;

    /**
     * @param conf
     */
    public RowTransform(RowTransformConfig conf) {
        super(conf);
        this.conf = conf;
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getStaticOutputSchema(request.inputSchema, request.getPrimitives());
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends RowTransformConfig {
        /**
         * 
         */
        private static final long serialVersionUID = 931309960123672568L;
        private Schema inputSchema;
    }
}
