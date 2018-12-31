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
import co.cask.cdap.api.data.schema.Schema.Type;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.common.enums.CorrelationCoefficient;
import co.cask.cdap.common.enums.CorrelationMatrixSchema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * SparkCompute plugin that generates Correlation coefficient for each pair of
 * columns within given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(CorrelationMatrixCompute.NAME)
@Description("Computes correlation coefficient for each pair of columns within given schema.")
public class CorrelationMatrixCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
    
    private static final Gson GSON_OBJ = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    /**
     * 
     */
    private static final long serialVersionUID = 3718869622401824253L;
    /**
     * 
     */
    public static final String NAME = "CorrelationMatrixCompute";
    private final Conf config;
    private int numPartitions;
    
    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {
                
        @Nullable
        @Description("Number of data partitions")
        private String numPartitions;
        
        Conf() {
            this.numPartitions = "10";
        }
        
        public int getNumPartitions() {
            try {
                if (this.numPartitions != null && !this.numPartitions.isEmpty()) {
                    return Integer.parseInt(numPartitions);
                }
            } catch (Exception e) {
                return 10;
            }
            return 10;
        }
    }
    
    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        this.numPartitions = config.getNumPartitions();
    }
    
    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }
    
    public CorrelationMatrixCompute(Conf config) {
        this.config = config;
    }
    
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }
    
    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getOutputSchema(request.inputSchema);
    }
    
    private Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();
        
        for (CorrelationMatrixSchema schema : CorrelationMatrixSchema.values()) {
            outputFields.add(Schema.Field.of(schema.getName(), 
                    Schema.nullableOf(Schema.of(getSchemaType(schema.getType())))));
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".correlation", outputFields);
    }
    
    private Type getSchemaType(final String type) {
        switch (type) {
            case "double":
                return Schema.Type.DOUBLE;
            case "long":
                return Schema.Type.LONG;
            case "string":
                return Schema.Type.STRING;
        }
        return null;
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
        javaRDD.repartition(this.numPartitions);
        Schema inputSchema = getInputSchema(javaRDD);
        final List<Schema.Field> inputField = inputSchema.getFields();
        Map<String, Integer> dataTypeCountMap = getDataTypeCountMap(inputField);
        Matrix pearsonCorrelMatrix = null;
        Matrix spearmanCorrelMatrix = null;
        List<StructuredRecord> recordList = new LinkedList<>();
        if (dataTypeCountMap.get("numeric") > 0) {
            JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
            pearsonCorrelMatrix = Statistics.corr(vectoredRDD.rdd(), CorrelationCoefficient.PEARSON.getName());
            spearmanCorrelMatrix = Statistics.corr(vectoredRDD.rdd(), CorrelationCoefficient.SPEARMAN.getName());
            double[] pearsonCoefficientValues = pearsonCorrelMatrix.toArray();
            double[] spearmanCoefficientValues = spearmanCorrelMatrix.toArray();
            List<String> computedFeatures = getComputedFeatures(inputField);
            
            Map<String, Map<String, Double>> computedCoefficientMatrixPearson = computeCoefficientMatrix(
                    pearsonCoefficientValues, computedFeatures);
            Map<String, Map<String, Double>> computedCoefficientMatrixSpearman = computeCoefficientMatrix(
                    spearmanCoefficientValues, computedFeatures);
            recordList = createStructuredRecord(computedCoefficientMatrixPearson, computedCoefficientMatrixSpearman,
                    inputSchema, computedFeatures);
        }
        return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
    }
    
    private Map<String, Map<String, Double>> computeCoefficientMatrix(double[] coefficientValues,
            List<String> computedFeatures) {
        int index = 0;
        Map<String, Map<String, Double>> computedCoefficientMatrix = new HashMap<>();
        for (String featureCol : computedFeatures) {
            for (String featureRow : computedFeatures) {
                Map<String, Double> matrix = computedCoefficientMatrix.get(featureRow);
                if (matrix == null) {
                    matrix = new HashMap<>();
                    computedCoefficientMatrix.put(featureRow, matrix);
                }
                matrix.put(featureCol, coefficientValues[index++]);
            }
        }
        return computedCoefficientMatrix;
    }
    
    private List<String> getComputedFeatures(final List<Field> inputField) {
        List<String> computedFeatures = new LinkedList<String>();
        for (Field field : inputField) {
            if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                computedFeatures.add(field.getName());
            }
        }
        return computedFeatures;
    }
    
    private Map<String, Integer> getDataTypeCountMap(final List<Field> inputField) {
        int stringCount = 0;
        int numericCount = 0;
        for (Field field : inputField) {
            Schema.Type type = getSchemaType(field.getSchema());
            if (type.equals(Schema.Type.STRING)) {
                stringCount++;
            } else if (type.equals(Schema.Type.DOUBLE) || type.equals(Schema.Type.INT) || type.equals(Schema.Type.FLOAT)
                    || type.equals(Schema.Type.LONG) || type.equals(Schema.Type.BOOLEAN)) {
                numericCount++;
            }
        }
        Map<String, Integer> dataTypeCountMap = new HashMap<>();
        dataTypeCountMap.put("numeric", numericCount);
        dataTypeCountMap.put("string", stringCount);
        return dataTypeCountMap;
    }
    
    private List<StructuredRecord> createStructuredRecord(
            Map<String, Map<String, Double>> computedCoefficientMatrixPearson,
            Map<String, Map<String, Double>> computedCoefficientMatrixSpearman, Schema inputSchema,
            List<String> computedFeatures) {
        List<StructuredRecord.Builder> builderList = new LinkedList<StructuredRecord.Builder>();
        Schema schema = getOutputSchema(inputSchema);
        for (Schema.Field field : inputSchema.getFields()) {
            if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                builderList.add(StructuredRecord.builder(schema));
                builderList.add(StructuredRecord.builder(schema));
            }
        }
        int index = 0;
        for (String featureName : computedFeatures) {
            Builder builder = builderList.get(index++);
            builder.set(CorrelationMatrixSchema.FEATURE_NAME.getName(), featureName);
            builder.set(CorrelationMatrixSchema.ID.getName(), (long) index);
            builder.set(CorrelationMatrixSchema.COEFFICIENT_TYPE.getName(), CorrelationCoefficient.PEARSON.getName());
            builder.set(CorrelationMatrixSchema.COEFFICIENT_SCORES.getName(),
                    GSON_OBJ.toJson(computedCoefficientMatrixPearson.get(featureName)));
            
            builder = builderList.get(index++);
            builder.set(CorrelationMatrixSchema.FEATURE_NAME.getName(), featureName);
            builder.set(CorrelationMatrixSchema.ID.getName(), (long) index);
            builder.set(CorrelationMatrixSchema.COEFFICIENT_TYPE.getName(), CorrelationCoefficient.SPEARMAN.getName());
            builder.set(CorrelationMatrixSchema.COEFFICIENT_SCORES.getName(),
                    GSON_OBJ.toJson(computedCoefficientMatrixSpearman.get(featureName)));
        }
        
        List<StructuredRecord> recordList = new LinkedList<>();
        for (Builder builder : builderList) {
            recordList.add(builder.build());
        }
        return recordList;
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
    
    private JavaRDD<Vector> getVectorRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {
        return javaRDD.map(new Function<StructuredRecord, Vector>() {
            
            @Override
            public Vector call(StructuredRecord record) throws Exception {
                List<Double> values = new LinkedList<Double>();
                for (Schema.Field field : inputField) {
                    if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                        Object val = record.get(field.getName());
                        if (val == null) {
                            values.add(0.0);
                            continue;
                        }
                        if (getSchemaType(field.getSchema()).equals(Schema.Type.BOOLEAN)) {
                            double valDouble = val.toString().equals("true") ? 1 : 0;
                            values.add(valDouble);
                            continue;
                        }
                        try {
                            values.add(Double.parseDouble(val.toString()));
                        } catch (Exception e) {
                            values.add(0.0);
                        }
                    }
                }
                double[] valDouble = new double[values.size()];
                int index = 0;
                for (Double val : values) {
                    valDouble[index++] = val;
                }
                return Vectors.dense(valDouble);
            }
        });
    }
    
    private Schema getInputSchema(JavaRDD<StructuredRecord> javaRDD) {
        Schema schema = javaRDD.first().getSchema();
        Map<String, Field> fieldMap = javaRDD.map(new Function<StructuredRecord, Map<String, Schema.Field>>() {
            
            @Override
            public Map<String, Field> call(StructuredRecord arg0) throws Exception {
                Map<String, Field> map = new HashMap<>();
                for (Field field : arg0.getSchema().getFields()) {
                    map.put(field.getName(), field);
                }
                return map;
            }
        }).reduce(new Function2<Map<String, Field>, Map<String, Field>, Map<String, Field>>() {
            
            @Override
            public Map<String, Field> call(Map<String, Field> arg0, Map<String, Field> arg1) throws Exception {
                Map<String, Field> map = new HashMap<>();
                map.putAll(arg0);
                map.putAll(arg1);
                return map;
            }
        });
        return Schema.recordOf(schema.getRecordName(), new LinkedList<Field>(fieldMap.values()));
    }
}
