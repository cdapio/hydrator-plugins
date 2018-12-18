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
package com.guavus.featureengineering.cdap.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import com.guavus.featureengineering.cdap.plugin.transform.config.FilterTransformConfig;
import com.guavus.featureengineering.cdap.plugin.transform.function.FilterFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = "transform")
@Name("FilterTransform")
@Description("Executes transform filters to filter records.")
public class FilterTransform extends Transform<StructuredRecord, StructuredRecord> {

    private final FilterTransformConfig conf;
    private List<FilterTransformConfig.FilterInfo> filterInfos;

    public FilterTransform(FilterTransformConfig conf) {
        this.conf = conf;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        Schema inputSchema = stageConfigurer.getInputSchema();
        // if null, the input schema is unknown, or its multiple schemas.
        if (inputSchema == null) {
            stageConfigurer.setOutputSchema(null);
            return;
        }

        stageConfigurer.setOutputSchema(getOutputSchema(inputSchema));
    }

    // initialize is called once at the start of each pipeline run
    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        filterInfos = conf.getFilters();
    }

    private Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>(inputSchema.getFields().size());
        outputFields.addAll(inputSchema.getFields());
        return Schema.recordOf(inputSchema.getRecordName() + ".filt", outputFields);
    }

    @Override
    public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) throws Exception {
        Schema inputSchema = valueIn.getSchema();
        Schema outputSchema = getOutputSchema(inputSchema);
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

        for (Schema.Field inputField : inputSchema.getFields()) {
            String inputFieldName = inputField.getName();
            Object inputVal = valueIn.get(inputFieldName);
            builder.set(inputFieldName, inputVal);
        }
        for (FilterTransformConfig.FilterInfo filterInfo : filterInfos) {
            Schema.Field inputField = inputSchema.getField(filterInfo.getField());
            FilterFunction filterFunction = filterInfo.getFilterFunction(inputField.getSchema());
            boolean result = filterFunction.applyFilter(valueIn);
            if (!result) {
                return;
            }
        }
        emitter.emit(builder.build());
    }

}
