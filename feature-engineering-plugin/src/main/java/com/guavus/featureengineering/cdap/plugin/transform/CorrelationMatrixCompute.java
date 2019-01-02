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
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationMatrixCompute.class);
    
    private static final Gson GSON_OBJ = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    private static ExecutorService threadPool = Executors.newCachedThreadPool();
    
    /**
     * 
     */
    private static final long serialVersionUID = 3718869622401824253L;
    /**
     * 
     */
    public static final String NAME = "CorrelationMatrixCompute";
    private final Conf config;
    private Set<String> discardedColumnSet;
    private int categoricalDictionaryThreshold;
    private int numPartitions;
    private int noOfParallelThreads;
    
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
        
        @Nullable
        @Description("Number of parallel threads to compute Chi Square Test")
        private String noOfParallelThreads;
        
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
        
        public int getNoOfParallelThreads() {
            try {
                if (this.noOfParallelThreads != null && !this.noOfParallelThreads.isEmpty()) {
                    return Integer.parseInt(noOfParallelThreads);
                }
            } catch (Exception e) {
                return 10;
            }
            return 10;
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
        this.noOfParallelThreads = config.getNoOfParallelThreads();
    }
    
    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }
    
    public CorrelationMatrixCompute(Conf config) {
        this.config = config;
        this.discardedColumnSet = new HashSet<String>();
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
            outputFields.add(
                    Schema.Field.of(schema.getName(), Schema.nullableOf(Schema.of(getSchemaType(schema.getType())))));
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
        javaRDD.cache();
        Schema inputSchema = getInputSchema(javaRDD);
        final List<Schema.Field> inputField = inputSchema.getFields();
        Map<String, Integer> dataTypeCountMap = getDataTypeCountMap(inputField);
        Matrix pearsonCorrelMatrix = null;
        Matrix spearmanCorrelMatrix = null;
        List<StructuredRecord> recordList = new LinkedList<>();
        if (dataTypeCountMap.get("numeric") > 0) {
            JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
            vectoredRDD.cache();
            
            pearsonCorrelMatrix = Statistics.corr(vectoredRDD.rdd(), CorrelationCoefficient.PEARSON.getName());
            spearmanCorrelMatrix = Statistics.corr(vectoredRDD.rdd(), CorrelationCoefficient.SPEARMAN.getName());
            double[] pearsonCoefficientValues = pearsonCorrelMatrix.toArray();
            double[] spearmanCoefficientValues = spearmanCorrelMatrix.toArray();
            List<String> computedFeatures = getComputedFeatures(inputField);
            vectoredRDD.unpersist();
            Map<String, Map<String, Double>> computedCoefficientMatrixPearson = computeCoefficientMatrix(
                    pearsonCoefficientValues, computedFeatures);
            Map<String, Map<String, Double>> computedCoefficientMatrixSpearman = computeCoefficientMatrix(
                    spearmanCoefficientValues, computedFeatures);
            recordList = createStructuredRecord(computedCoefficientMatrixPearson, computedCoefficientMatrixSpearman,
                    inputSchema, computedFeatures);
        } else if (dataTypeCountMap.get("string") > 0) {
            List<Tuple2<String, Map<String, Integer>>> columnValueFrequencyTupleList = getColumnValueFrequencies(
                    javaRDD, inputField);
            List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList = mapStringValuesToIndex(
                    columnValueFrequencyTupleList);
            addChiSquareTestOfIndependenceRecords(columnValueIndexTupleList, recordList, inputSchema, javaRDD);
        }
        javaRDD.unpersist();
        return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
    }
    
    private void addChiSquareTestOfIndependenceRecords(
            List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList, List<StructuredRecord> recordList,
            Schema inputSchema, JavaRDD<StructuredRecord> javaRDD) {
        int taskSize = columnValueIndexTupleList.size() / this.noOfParallelThreads;
        if (taskSize == 0) {
            taskSize = 1;
        }
        int index = 0;
        List<Future<Map<String, Map<String, ChiSqTestResult>>>> futureList = new LinkedList<>();
        List<String> taskFieldList = new LinkedList<>();
        Map<String, Map<String, ChiSqTestResult>> mergedResult = new HashMap<>();
        for (Tuple2<String, Map<String, Integer>> tuple : columnValueIndexTupleList) {
            if (index < taskSize) {
                taskFieldList.add(tuple._1);
                index++;
            } else {
                Future<Map<String, Map<String, ChiSqTestResult>>> futureTask = threadPool.submit(new ChiSquaredTestJob(
                        columnValueIndexTupleList, javaRDD, inputSchema.getFields(), taskFieldList));
                futureList.add(futureTask);
                taskFieldList = new LinkedList<>();
                taskFieldList.add(tuple._1);
                index = 1;
            }
        }
        
        for (Future<Map<String, Map<String, ChiSqTestResult>>> task : futureList) {
            try {
                Map<String, Map<String, ChiSqTestResult>> result = task.get();
                mergedResult.putAll(result);
            } catch (Exception e) {
                LOG.error("Got error while computing chi-square test result", e);
            }
            
        }
        generateRecordsFromChiSqTestResult(mergedResult, recordList, inputSchema);
    }
    
    private void generateRecordsFromChiSqTestResult(Map<String, Map<String, ChiSqTestResult>> mergedResult,
            List<StructuredRecord> recordList, Schema inputSchema) {
        Schema outputSchema = getOutputSchema(inputSchema);
        int index = recordList.size() + 1;
        for (Map.Entry<String, Map<String, ChiSqTestResult>> resultEntry : mergedResult.entrySet()) {
            Builder builder = StructuredRecord.builder(outputSchema);
            builder.set(CorrelationMatrixSchema.ID.getName(), (long) index);
            builder.set(CorrelationMatrixSchema.FEATURE_NAME.getName(), resultEntry.getKey());
            builder.set(CorrelationMatrixSchema.COEFFICIENT_TYPE.getName(),
                    CorrelationCoefficient.CHI_SQUARE_TEST.getName());
            builder.set(CorrelationMatrixSchema.COEFFICIENT_SCORES.getName(),
                    GSON_OBJ.toJson(createChiSqResultMap(resultEntry.getValue())));
            index++;
            recordList.add(builder.build());
        }
    }
    
    private Object createChiSqResultMap(final Map<String, ChiSqTestResult> resultMap) {
        Map<String, ChiSqResult> outputMap = new HashMap<>();
        for (Map.Entry<String, ChiSqTestResult> entry : resultMap.entrySet()) {
            ChiSqTestResult testResult = entry.getValue();
            ChiSqResult result = new ChiSqResult();
            result.setDegreeOfFreedom(testResult.degreesOfFreedom());
            result.setNullHypothesis(testResult.nullHypothesis());
            result.setpValue(testResult.pValue());
            result.setStatistic(testResult.statistic());
            outputMap.put(entry.getKey(), result);
        }
        return outputMap;
    }
    
    private static class ChiSquaredTestJob implements Callable<Map<String, Map<String, ChiSqTestResult>>> {
        private List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList;
        private List<Field> inputField;
        private JavaRDD<StructuredRecord> javaRDD;
        private List<String> taskFieldList;
        
        ChiSquaredTestJob(List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList,
                JavaRDD<StructuredRecord> javaRDD, List<Field> inputField, List<String> taskFieldList) {
            this.columnValueIndexTupleList = columnValueIndexTupleList;
            this.javaRDD = javaRDD;
            this.inputField = inputField;
            this.taskFieldList = taskFieldList;
        }
        
        @Override
        public Map<String, Map<String, ChiSqTestResult>> call() throws Exception {
            Map<String, Map<String, ChiSqTestResult>> result = new HashMap<>();
            for (String targetField : taskFieldList) {
                JavaRDD<LabeledPoint> labelledRDD = getLabeledRDD(javaRDD, columnValueIndexTupleList, inputField,
                        targetField);
                labelledRDD.cache();
                ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(labelledRDD.rdd());
                Map<String, ChiSqTestResult> targetResult = new HashMap<>();
                result.put(targetField, targetResult);
                int index = 0;
                for (Tuple2<String, Map<String, Integer>> tuple : columnValueIndexTupleList) {
                    if (tuple._1.equals(targetField)) {
                        continue;
                    }
                    targetResult.put(tuple._1, featureTestResults[index++]);
                }
            }
            return result;
        }
        
        private JavaRDD<LabeledPoint> getLabeledRDD(JavaRDD<StructuredRecord> javaRDD,
                List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList, List<Field> inputField,
                String targetField) {
            return javaRDD.map(new Function<StructuredRecord, LabeledPoint>() {
                
                @Override
                public LabeledPoint call(StructuredRecord record) throws Exception {
                    double targetDoubleVal = 0.0;
                    List<Double> valueList = new LinkedList<>();
                    for (Tuple2<String, Map<String, Integer>> tuple : columnValueIndexTupleList) {
                        Object val = record.get(tuple._1);
                        String stringVal = "null";
                        if (val != null) {
                            stringVal = val.toString();
                        }
                        Double doubleVal = 0.0;
                        Map<String, Integer> indexMap = tuple._2;
                        if (indexMap.containsKey(stringVal)) {
                            doubleVal = 1.0 * indexMap.get(stringVal);
                        }
                        if (tuple._1.equals(targetField)) {
                            targetDoubleVal = doubleVal;
                        } else {
                            valueList.add(doubleVal);
                        }
                    }
                    
                    double[] values = new double[valueList.size()];
                    int index = 0;
                    for (double value : valueList) {
                        values[index++] = value;
                    }
                    return new LabeledPoint(targetDoubleVal, Vectors.dense(values));
                }
            });
        }
        
    }
    
    private List<Tuple2<String, Map<String, Integer>>> mapStringValuesToIndex(
            List<Tuple2<String, Map<String, Integer>>> columnValueFrequencyTupleList) {
        List<Tuple2<String, Map<String, Integer>>> columnValueIndexTupleList = new LinkedList<>();
        for (Tuple2<String, Map<String, Integer>> columnValueIndexTuple : columnValueFrequencyTupleList) {
            if (columnValueIndexTuple._2.size() > this.categoricalDictionaryThreshold) {
                continue;
            }
            Map<String, Integer> indexMap = getIndexMap(columnValueIndexTuple._2);
            columnValueIndexTupleList.add(new Tuple2<String, Map<String, Integer>>(columnValueIndexTuple._1, indexMap));
        }
        return columnValueFrequencyTupleList;
    }
    
    private Map<String, Integer> getIndexMap(Map<String, Integer> frequencyMap) {
        List<Tuple2<Integer, String>> indexTupleList = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
            indexTupleList.add(new Tuple2<Integer, String>(entry.getValue(), entry.getKey()));
        }
        Collections.sort(indexTupleList, new Comparator<Tuple2<Integer, String>>() {
            
            @Override
            public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
                return -1 * o1._1.compareTo(o2._1);
            }
        });
        Map<String, Integer> indexMap = new HashMap<>();
        int index = 1;
        for (Tuple2<Integer, String> tuple : indexTupleList) {
            indexMap.put(tuple._2, index++);
        }
        return indexMap;
    }
    
    private List<Tuple2<String, Map<String, Integer>>> getColumnValueFrequencies(JavaRDD<StructuredRecord> javaRDD,
            List<Field> inputField) {
        List<Tuple2<String, Map<String, Integer>>> fieldMap = null;
        javaRDD.map(new Function<StructuredRecord, List<Tuple2<String, Map<String, Integer>>>>() {
            
            @Override
            public List<Tuple2<String, Map<String, Integer>>> call(StructuredRecord record) throws Exception {
                List<Tuple2<String, Map<String, Integer>>> valueMapList = new LinkedList<>();
                
                for (Field field : inputField) {
                    if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                            && !discardedColumnSet.contains(field.getName())) {
                        Object val = record.get(field.getName());
                        String stringVal = "null";
                        if (val != null) {
                            stringVal = val.toString();
                        }
                        Map<String, Integer> tempMap = new HashMap<String, Integer>();
                        stringVal = normalizeDictionaryValue(stringVal);
                        tempMap.put(stringVal, 1);
                        valueMapList.add(new Tuple2<String, Map<String, Integer>>(field.getName(), tempMap));
                    }
                }
                return valueMapList;
            }
            
            private String normalizeDictionaryValue(String input) {
                return input.replaceAll("\\s+", "^");
            }
        }).reduce(
                new Function2<List<Tuple2<String, Map<String, Integer>>>, List<Tuple2<String, Map<String, Integer>>>, 
                List<Tuple2<String, Map<String, Integer>>>>() {
                    
                    @Override
                    public List<Tuple2<String, Map<String, Integer>>> call(
                            List<Tuple2<String, Map<String, Integer>>> input1,
                            List<Tuple2<String, Map<String, Integer>>> input2) throws Exception {
                        int index = 0;
                        List<Tuple2<String, Map<String, Integer>>> mergedValueList = new LinkedList<>();
                        
                        for (Field field : inputField) {
                            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                                    && !discardedColumnSet.contains(field.getName())) {
                                Tuple2<String, Map<String, Integer>> tuple1 = input1.get(index);
                                Tuple2<String, Map<String, Integer>> tuple2 = input2.get(index++);
                                if (!tuple1._1.equals(tuple2._1) || !tuple1._1.equals(field.getName())) {
                                    LOG.error("Tuples are mismatching. There is some problem in the Chi Squared Test of"
                                            + " Independence for tuples = " + tuple1._1 + " and tuple = " + tuple2._1);
                                }
                                Map<String, Integer> mergedMap = mergeMaps(tuple1._2, tuple2._2);
                                mergedValueList
                                        .add(new Tuple2<String, Map<String, Integer>>(field.getName(), mergedMap));
                            }
                        }
                        return mergedValueList;
                    }
                    
                    private Map<String, Integer> mergeMaps(Map<String, Integer> map1, Map<String, Integer> map2) {
                        Map<String, Integer> mergedMap = new HashMap<>();
                        mergedMap.putAll(map1);
                        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
                            if (mergedMap.containsKey(entry.getKey())) {
                                Integer frequency1 = mergedMap.get(entry.getKey());
                                mergedMap.put(entry.getKey(), frequency1 + entry.getValue());
                            } else {
                                mergedMap.put(entry.getKey(), entry.getValue());
                            }
                        }
                        return mergedMap;
                    }
                });
        
        return fieldMap;
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
    
    private static class ChiSqResult {
        private int degreeOfFreedom;
        private String nullHypothesis;
        private double pValue;
        private double statistic;
        
        /**
         * @return the degreeOfFreedom
         */
        public int getDegreeOfFreedom() {
            return degreeOfFreedom;
        }
        
        /**
         * @param degreeOfFreedom
         *            the degreeOfFreedom to set
         */
        public void setDegreeOfFreedom(int degreeOfFreedom) {
            this.degreeOfFreedom = degreeOfFreedom;
        }
        
        /**
         * @return the nullHypothesis
         */
        public String getNullHypothesis() {
            return nullHypothesis;
        }
        
        /**
         * @param nullHypothesis
         *            the nullHypothesis to set
         */
        public void setNullHypothesis(String nullHypothesis) {
            this.nullHypothesis = nullHypothesis;
        }
        
        /**
         * @return the pValue
         */
        public double getpValue() {
            return pValue;
        }
        
        /**
         * @param pValue
         *            the pValue to set
         */
        public void setpValue(double pValue) {
            this.pValue = pValue;
        }
        
        /**
         * @return the statistic
         */
        public double getStatistic() {
            return statistic;
        }
        
        /**
         * @param statistic
         *            the statistic to set
         */
        public void setStatistic(double statistic) {
            this.statistic = statistic;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + degreeOfFreedom;
            result = prime * result + ((nullHypothesis == null) ? 0 : nullHypothesis.hashCode());
            long temp;
            temp = Double.doubleToLongBits(pValue);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(statistic);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ChiSqResult other = (ChiSqResult) obj;
            if (degreeOfFreedom != other.degreeOfFreedom) {
                return false;
            }
            if (nullHypothesis == null) {
                if (other.nullHypothesis != null) {
                    return false;
                }
            } else if (!nullHypothesis.equals(other.nullHypothesis)) {
                return false;
            }
            if (Double.doubleToLongBits(pValue) != Double.doubleToLongBits(other.pValue)) {
                return false;
            }
            if (Double.doubleToLongBits(statistic) != Double.doubleToLongBits(other.statistic)) {
                return false;
            }
            return true;
        }
        
        @Override
        public String toString() {
            return "ChiSqResult [degreeOfFreedom=" + degreeOfFreedom + ", nullHypothesis=" + nullHypothesis
                    + ", pValue=" + pValue + ", statistic=" + statistic + "]";
        }
        
    }
}
