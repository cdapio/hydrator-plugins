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
import co.cask.cdap.api.data.format.StructuredRecord.Builder;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.Path;

/**
 * SparkCompute plugin that generates different stats for given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(RowDeduper.NAME)
@Description("Create Deduped column for target schema column.")
public class RowDeduper extends SparkCompute<StructuredRecord, StructuredRecord> {
    /**
     * 
     */
    private static final long serialVersionUID = 8592939878395242673L;
    public static final String NAME = "RowDeduper";
    private final Conf config;
    private String columnToBeDeduped;

    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {

        @Description("The field from the input records containing the words to count.")
        private String columnToBeDeduped;

        Conf(String columnToBeDeduped) {
            this.columnToBeDeduped = columnToBeDeduped;
        }

        Conf() {
            this.columnToBeDeduped = "";
        }

        String getColumnToBeDeduped() {
            Iterable<String> column = Splitter.on(',').trimResults().split(columnToBeDeduped);
            try {
                return column.iterator().next();
            } catch (Exception e) {
                return "";
            }
        }
    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        this.columnToBeDeduped = config.getColumnToBeDeduped();
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }

    public RowDeduper(Conf config) {
        this.config = config;
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
        Schema outputSchema = getOutputSchema(inputSchema, config.getColumnToBeDeduped());
        // set the output schema so downstream stages will know their input schema.
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getOutputSchema(request.inputSchema, request.getColumnToBeDeduped());
    }

    private Schema getOutputSchema(Schema inputSchema, String columnToBeDeduped) {
        List<Schema.Field> outputFields = new ArrayList<>();
        for (Schema.Field field : inputSchema.getFields()) {
            if (columnToBeDeduped.equals(field.getName())) {
                outputFields.add(Schema.Field.of(field.getName(), field.getSchema()));
            }
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".deduper", outputFields);
    }

    @Override
    public JavaRDD<StructuredRecord> transform(final SparkExecutionPluginContext sparkExecutionPluginContext,
            JavaRDD<StructuredRecord> javaRDD) throws Exception {
        long size = javaRDD.count();
        if (size == 0) {
            return sparkExecutionPluginContext.getSparkContext().parallelize(new LinkedList<StructuredRecord>());
        }
        try {
            javaRDD.first();
        } catch (Throwable th) {
            return sparkExecutionPluginContext.getSparkContext().parallelize(new LinkedList<StructuredRecord>());
        }
        Schema outputSchema = getOutputSchema(sparkExecutionPluginContext.getInputSchema(), columnToBeDeduped);
        List<Field> outputField = outputSchema.getFields();
        JavaRDD<StructuredRecord> outputRDD = javaRDD.map(new Function<StructuredRecord, String>() {

            @Override
            public String call(StructuredRecord record) throws Exception {
                for (Schema.Field field : outputField) {
                    if (field.getName().equals(columnToBeDeduped)) {
                        return record.get(field.getName()).toString();
                    }
                }
                return "";
            }
        }).distinct().map(new Function<String, StructuredRecord>() {

            @Override
            public StructuredRecord call(String arg0) throws Exception {
                Builder builder = StructuredRecord.builder(outputSchema);
                Schema.Type type = getSchemaType(outputSchema.getField(columnToBeDeduped).getSchema());
                if (type.equals(Schema.Type.DOUBLE)) {
                    builder.set(columnToBeDeduped, Double.parseDouble(arg0));
                } else if (type.equals(Schema.Type.LONG)) {
                    builder.set(columnToBeDeduped, Long.parseLong(arg0));
                } else if (type.equals(Schema.Type.INT)) {
                    builder.set(columnToBeDeduped, Integer.parseInt(arg0));
                } else if (type.equals(Schema.Type.FLOAT)) {
                    builder.set(columnToBeDeduped, Float.parseFloat(arg0));
                } else if (type.equals(Schema.Type.BOOLEAN)) {
                    builder.set(columnToBeDeduped, Boolean.parseBoolean(arg0));
                } else if (type.equals(Schema.Type.STRING)) {
                    builder.set(columnToBeDeduped, arg0);
                }
                return builder.build();
            }

        });
        
        outputRDD.cache();
        return outputRDD;
    }

    private Schema.Type getSchemaType(Schema schema) {
        if (schema.getType().equals(Schema.Type.UNION)) {
            List<Schema> schemas = schema.getUnionSchemas();
            if (schemas.size() == 2) {
                if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
                    return schemas.get(1).getType();
                } else {
                    return schemas.get(0).getType();
                }
            }
            return schema.getType();
        } else {
            return schema.getType();
        }
    }
}
