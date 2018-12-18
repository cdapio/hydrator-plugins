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

import com.guavus.featureengineering.cdap.plugin.transform.config.RowTransformConfig;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
@Plugin(type = "transform")
@Name("VariableOutputSchemaRowTransform")
@Description("Executes transform primitives to add new columns in record.")
public class VariableOutputSchemaRowTransform extends Transform<StructuredRecord, StructuredRecord> {

    private final RowTransformConfig conf;

    public VariableOutputSchemaRowTransform(RowTransformConfig conf) {
        this.conf = conf;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        stageConfigurer.setOutputSchema(null);
    }

    // initialize is called once at the start of each pipeline run
    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) throws Exception {
        Schema inputSchema = valueIn.getSchema();

        List<Schema.Field> outputFields = new ArrayList<>();
        List<Schema.Field> newlyAddedFields = new LinkedList<>();
        List<Integer> newlyAddedValues = new LinkedList<Integer>();
        outputFields.addAll(inputSchema.getFields());

        for (Schema.Field inputField : inputSchema.getFields()) {
            String inputFieldName = inputField.getName();
            if (inputField.getSchema().getType().equals(Schema.Type.INT)) {
                Schema.Field newField = Schema.Field.of(inputFieldName + "_r", Schema.of(Schema.Type.INT));
                outputFields.add(newField);
                newlyAddedFields.add(newField);
                Object inputVal = valueIn.get(inputFieldName);
                newlyAddedValues.add(((Integer) inputVal) * 2);
            }
        }
        Schema outputSchema = Schema.recordOf(inputSchema.getRecordName() + ".variableTrans", outputFields);
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

        for (Schema.Field inputField : inputSchema.getFields()) {
            String inputFieldName = inputField.getName();
            Object inputVal = valueIn.get(inputFieldName);
            builder.set(inputFieldName, inputVal);
        }
        for (int i = 0; i < newlyAddedFields.size(); i++) {
            builder.set(newlyAddedFields.get(i).getName(), newlyAddedValues.get(i));
        }
        emitter.emit(builder.build());
    }

}
