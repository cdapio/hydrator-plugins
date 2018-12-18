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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import com.guavus.featureengineering.cdap.plugin.transform.config.RowTransformConfigBase;
import com.guavus.featureengineering.cdap.plugin.transform.config.RowTransformConfigBase.FunctionInfo;
import com.guavus.featureengineering.cdap.plugin.transform.function.TransformFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public abstract class RowTransformBase extends Transform<StructuredRecord, StructuredRecord> {

    private final RowTransformConfigBase conf;
    private List<RowTransformConfigBase.FunctionInfo> functionInfos;
    private Map<String, TransformFunction> transformFunctionsMap;

    public RowTransformBase(RowTransformConfigBase conf) {
        super();
        this.conf = conf;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        Schema inputSchema = stageConfigurer.getInputSchema();
        // if null, the input schema is unknown, or its multiple schemas.
        if (inputSchema == null || conf.isDynamicSchema) {
            stageConfigurer.setOutputSchema(null);
            return;
        }
        List<FunctionInfo> transformFunctions = conf.getPrimitives();
        // otherwise, we have a constant input schema. Get the output schema and
        // propagate the schema, which is group by fields + aggregate fields
        stageConfigurer.setOutputSchema(getStaticOutputSchema(inputSchema, transformFunctions));
    }

    protected Schema getStaticOutputSchema(Schema inputSchema, List<FunctionInfo> transformFunctions) {
        List<Schema.Field> outputFields = new ArrayList<>(inputSchema.getFields().size() + transformFunctions.size());
        outputFields.addAll(inputSchema.getFields());
        transformFunctionsMap = new LinkedHashMap<String, TransformFunction>();
        // check that all fields needed by aggregate functions exist in the input
        // schema.
        for (FunctionInfo functionInfo : transformFunctions) {
            Schema[] inputFieldSchema = new Schema[functionInfo.getField().length];
            int index = 0;
            for (String functionField : functionInfo.getField()) {
                Schema.Field inputField = inputSchema.getField(functionField);
                if (inputField == null) {
                    throw new IllegalArgumentException(String.format(
                            "Invalid transformFunction %s(%s): Field '%s' does not exist in input schema %s.",
                            functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
                }
                inputFieldSchema[index++] = inputField.getSchema();
            }

            TransformFunction transformFunction = functionInfo.getTransformFunction(inputFieldSchema);
            outputFields.add(Schema.Field.of(functionInfo.getName(), transformFunction.getOutputSchema()));
            transformFunctionsMap.put(functionInfo.getName(), transformFunction);
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".trans", outputFields);
    }

    // initialize is called once at the start of each pipeline run
    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        functionInfos = conf.getPrimitives();
    }

    protected Schema getDynamicOutputSchema(Schema inputSchema, List<FunctionInfo> transformFunctions) {
        List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.addAll(inputSchema.getFields());
        transformFunctionsMap = new LinkedHashMap<String, TransformFunction>();
        // check that all fields needed by aggregate functions exist in the input
        // schema.
        for (FunctionInfo functionInfo : transformFunctions) {
            Schema[] inputFieldSchema = new Schema[functionInfo.getField().length];

            int index = 0;
            for (String functionField : functionInfo.getField()) {
                Schema.Field inputField = inputSchema.getField(functionField);
                if (inputField == null) {
                    if (functionInfo.getField().length == 1) {
                        // could be the case that column is categorical column and exists in extended
                        // form along with dictionary in schema.
                        List<Field> matchingFields = getAllMatchingSchemaFields(inputSchema.getFields(),
                                functionInfo.getField()[0]);
                        for (Field matchingField : matchingFields) {
                            String outputFieldName = functionInfo.getFunction().toLowerCase() + "_"
                                    + matchingField.getName() + "_";
                            Schema[] tempSchema = new Schema[1];
                            tempSchema[0] = matchingField.getSchema();
                            TransformFunction transformFunction = functionInfo.getTransformFunction(tempSchema);
                            outputFields.add(Schema.Field.of(outputFieldName, transformFunction.getOutputSchema()));
                            transformFunctionsMap.put(outputFieldName, transformFunction);
                        }
                    }
                    continue;
                }
                inputFieldSchema[index++] = inputField.getSchema();
            }
            if (index > 0) {
                TransformFunction transformFunction = functionInfo.getTransformFunction(inputFieldSchema);
                outputFields.add(Schema.Field.of(functionInfo.getName(), transformFunction.getOutputSchema()));
                transformFunctionsMap.put(functionInfo.getName(), transformFunction);
            }
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".trans", outputFields);
    }

    protected List<Field> getAllMatchingSchemaFields(List<Field> fields, String fieldName) {
        List<Field> matchingFields = new LinkedList<Field>();
        for (Field field : fields) {
            if (field.getName().contains(fieldName)) {
                matchingFields.add(field);
            }
        }
        return matchingFields;
    }

    @Override
    public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) throws Exception {
        Schema inputSchema = valueIn.getSchema();
        Schema outputSchema = null;
        if (!conf.isDynamicSchema) {
            outputSchema = getStaticOutputSchema(inputSchema, functionInfos);
        } else {
            outputSchema = getDynamicOutputSchema(inputSchema, functionInfos);
        }
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

        for (Schema.Field inputField : inputSchema.getFields()) {
            String inputFieldName = inputField.getName();
            Object inputVal = valueIn.get(inputFieldName);
            builder.set(inputFieldName, inputVal);
        }

        for (Map.Entry<String, TransformFunction> entry : transformFunctionsMap.entrySet()) {
            builder.set(entry.getKey(), entry.getValue().applyFunction(valueIn));
        }
        emitter.emit(builder.build());
    }

}
