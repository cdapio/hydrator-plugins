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
import co.cask.cdap.common.enums.CategoricalColumnEncoding;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * SparkCompute plugin that generates different stats for given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(OneHotEncoder.NAME)
@Description("Computes One Hot Encoding for each string schema column.")
public class OneHotEncoder extends SparkCompute<StructuredRecord, StructuredRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OneHotEncoder.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 3718869622401824253L;
    /**
     * 
     */
    public static final String NAME = "OneHotEncoder";
    private final Conf config;
    private Set<String> discardedColumnSet;
    private int categoricalDictionaryThreshold;
    private int numPartitions;
    
    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {
        
        @Nullable
        @Description("String Columns to be discarded from encoding.")
        private String discardedColumns;
        
        @Nullable
        @Description("Threshold to be used while discarding string columns with cardinality more than this threshold")
        private String categoricalDictionaryThreshold;
        
        @Nullable
        @Description("Number of data partitions")
        private String numPartitions;
        
        Conf() {
            this.discardedColumns = "";
            this.categoricalDictionaryThreshold = "";
            this.numPartitions = "10";
        }
        
        public List<String> getDiscardedColumns() {
            List<String> fields = new ArrayList<>();
            for (String field : Splitter.on(',').trimResults().split(discardedColumns)) {
                fields.add(field);
            }
            return fields;
        }
        
        public int getCategoricalDictionaryThreshold() {
            if (this.categoricalDictionaryThreshold == null || this.categoricalDictionaryThreshold.isEmpty()) {
                return 1000;
            }
            try {
                return Integer.parseInt(this.categoricalDictionaryThreshold.trim());
            } catch (Exception e) {
                return 1000;
            }
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
        this.discardedColumnSet.addAll(config.getDiscardedColumns());
        this.categoricalDictionaryThreshold = config.getCategoricalDictionaryThreshold();
        this.numPartitions = config.getNumPartitions();
    }
    
    public OneHotEncoder(Conf config) {
        this.config = config;
        this.discardedColumnSet = new HashSet<String>();
    }
    
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
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
        if (dataTypeCountMap.get("string") == null || dataTypeCountMap.get("string") <= 0) {
            return javaRDD;
        }
        
        List<Tuple2<String, Set<String>>> flattenedInputSchemaTupleList = getEncodedSchema(javaRDD, inputField);
        Schema outputSchema = getOutputSchema(inputSchema, flattenedInputSchemaTupleList);
        JavaRDD<StructuredRecord> outputRDD = generateOutput(inputSchema, outputSchema, javaRDD);
        outputRDD.cache();
        return outputRDD;
    }
    
    private JavaRDD<StructuredRecord> generateOutput(Schema inputSchema, Schema outputSchema,
            JavaRDD<StructuredRecord> javaRDD) {
        return javaRDD.map(new Function<StructuredRecord, StructuredRecord>() {
            
            @Override
            public StructuredRecord call(StructuredRecord record) throws Exception {
                Builder builder = StructuredRecord.builder(outputSchema);
                for (Field outputField : outputSchema.getFields()) {
                    Field inputField = inputSchema.getField(outputField.getName());
                    Object val = record.get(outputField.getName());
                    if (inputField == null) {
                        String[] token = outputField.getName()
                                .split(CategoricalColumnEncoding.DUMMY_CODING.getStringEncoding());
                        inputField = inputSchema.getField(token[0]);
                        val = record.get(inputField.getName());
                        if (val == null) {
                            val = "null";
                        }
                        if (token[1].equals(val.toString())) {
                            builder.set(outputField.getName(), 1);
                        } else {
                            builder.set(outputField.getName(), 0);
                        }
                    } else {
                        builder.set(inputField.getName(), val);
                    }
                }
                return builder.build();
            }
            
        });
    }
    
    private Schema getOutputSchema(Schema inputSchema,
            List<Tuple2<String, Set<String>>> flattenedInputSchemaTupleList) {
        List<Schema.Field> outputFields = new ArrayList<>();
        int index = 0;
        for (Field field : inputSchema.getFields()) {
            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                    && !discardedColumnSet.contains(field.getName())) {
                Tuple2<String, Set<String>> flattenedColumnTuple = flattenedInputSchemaTupleList.get(index++);
                if (!flattenedColumnTuple._1.equals(field.getName())) {
                    throw new IllegalStateException(
                            "Input schema column not found while flattening input schema for column = "
                                    + field.getName());
                }
                if (flattenedColumnTuple._2.size() > this.categoricalDictionaryThreshold) {
                    if (field.getSchema().isNullable()) {
                        outputFields.add(field);
                    } else {
                        outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(field.getSchema())));
                    }
                    continue;
                }
                boolean skipFirstColumn = true;
                for (String dictValue : flattenedColumnTuple._2) {
                    if (skipFirstColumn) {
                        skipFirstColumn = false;
                        continue;
                    }
                    outputFields.add(Schema.Field.of(getFlattenedColumnName(field.getName(), dictValue),
                            Schema.nullableOf(Schema.of(Schema.Type.INT))));
                }
            } else {
                if (field.getSchema().isNullable()) {
                    outputFields.add(field);
                } else {
                    outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(field.getSchema())));
                }
            }
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".hotenc", outputFields);
        
    }
    
    private String getFlattenedColumnName(String name, String dictValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(CategoricalColumnEncoding.DUMMY_CODING.getStringEncoding());
        sb.append(dictValue);
        return sb.toString();
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
    
    private List<Tuple2<String, Set<String>>> getEncodedSchema(JavaRDD<StructuredRecord> javaRDD,
            List<Field> inputField) {
        
        List<Tuple2<String, Set<String>>> fieldMap = javaRDD
                .map(new Function<StructuredRecord, List<Tuple2<String, Set<String>>>>() {
                    
                    @Override
                    public List<Tuple2<String, Set<String>>> call(StructuredRecord record) throws Exception {
                        List<Tuple2<String, Set<String>>> valueMapList = new LinkedList<>();
                        
                        for (Field field : inputField) {
                            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                                    && !discardedColumnSet.contains(field.getName())) {
                                Object val = record.get(field.getName());
                                String stringVal = "null";
                                if (val != null) {
                                    stringVal = val.toString();
                                }
                                Set<String> tempSet = new HashSet<String>();
                                stringVal = normalizeDictionaryValue(stringVal);
                                tempSet.add(stringVal);
                                valueMapList.add(new Tuple2<String, Set<String>>(field.getName(), tempSet));
                            }
                        }
                        return valueMapList;
                    }
                    
                    private String normalizeDictionaryValue(String input) {
                        return input.replaceAll("\\s+", "^");
                    }
                })
                .reduce(new Function2<List<Tuple2<String, Set<String>>>, List<Tuple2<String, Set<String>>>, 
                        List<Tuple2<String, Set<String>>>>() {
                    
                    @Override
                    public List<Tuple2<String, Set<String>>> call(List<Tuple2<String, Set<String>>> input1,
                            List<Tuple2<String, Set<String>>> input2) throws Exception {
                        int index = 0;
                        List<Tuple2<String, Set<String>>> mergedValueList = new LinkedList<>();
                        
                        for (Field field : inputField) {
                            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                                    && !discardedColumnSet.contains(field.getName())) {
                                Tuple2<String, Set<String>> tuple1 = input1.get(index);
                                Tuple2<String, Set<String>> tuple2 = input2.get(index++);
                                if (!tuple1._1.equals(tuple2._1) || !tuple1._1.equals(field.getName())) {
                                    LOG.error("Tuples are mismatching. There is some problem in the HotEncoding of "
                                            + "tuples = " + tuple1._1 + " and tuple = " + tuple2._1);
                                }
                                Set<String> mergedSet = new HashSet<String>();
                                mergedSet.addAll(tuple1._2);
                                mergedSet.addAll(tuple2._2);
                                mergedValueList.add(new Tuple2<String, Set<String>>(field.getName(), mergedSet));
                            }
                        }
                        return mergedValueList;
                    }
                });
        
        return fieldMap;
    }
}
