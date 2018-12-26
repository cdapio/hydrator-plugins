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
import co.cask.cdap.common.enums.VarianceInflationFactorSchema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
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
import javax.ws.rs.Path;

/**
 * SparkCompute plugin that generates Correlation coefficient for each pair of
 * columns within given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(VarianceInflationFactorCompute.NAME)
@Description("Computes variance inflation factor for each feature with respect to other features as predictors. Then "
        + "discard features with highest vif score each iteration till all features have vif score less then provided "
        + "threshold")
public class VarianceInflationFactorCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(VarianceInflationFactorCompute.class);
    
    private static final Gson GSON_OBJ = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    private static final Double ZERO = 0.0;
    
    /**
     * 
     */
    private static final long serialVersionUID = 3718869622401824253L;
    /**
     * 
     */
    public static final String NAME = "VarianceInflationFactorCompute";
    private final Conf config;
    private double selectionThreshold;
    private int numIterations;
    private int linearRegressionIterations;
    private double stepSize;
    
    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {
        
        @Nullable
        @Description("Min threshold to select features with greater vif score.")
        private String selectionThreshold;
        
        @Nullable
        @Description("Number of iterations to run  with greater vif score.")
        private String numIterations;
        
        @Nullable
        @Description("Number of Iterations to run multi linear regression for.")
        private String linearRegressionIterations;
        
        @Nullable
        @Description("Step size for multi linear regression run.")
        private String stepSize;
        
        Conf() {
            this.selectionThreshold = "0.999999999";
            this.numIterations = "10000";
            this.linearRegressionIterations = "100";
            this.stepSize = "0.00000001";
        }
        
        public Double getSelectionThreshold() {
            try {
                return Double.parseDouble(this.selectionThreshold);
            } catch (Throwable th) {
                LOG.error("Enable to parse selectionThreshold value = " + this.selectionThreshold, th);
            }
            return 0.999999999;
        }
        
        public int getLinearRegressionIterations() {
            try {
                return Integer.parseInt(this.linearRegressionIterations);
            } catch (Throwable th) {
                LOG.error("Enable to parse linear regression iteration value = " + this.linearRegressionIterations);
            }
            return Integer.MAX_VALUE / 2;
        }
        
        public int getNumIterations() {
            try {
                return Integer.parseInt(this.numIterations);
            } catch (Throwable th) {
                return 10000;
            }
        }
        
        public double getStepSize() {
            try {
                return Double.parseDouble(this.stepSize);
            } catch (Throwable th) {
                return 0.0001;
            }
        }
    }
    
    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        this.selectionThreshold = config.getSelectionThreshold();
        this.numIterations = config.getNumIterations();
        this.linearRegressionIterations = config.getLinearRegressionIterations();
        this.stepSize = config.getStepSize();
    }
    
    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }
    
    public VarianceInflationFactorCompute(Conf config) {
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
        
        for (VarianceInflationFactorSchema schema : VarianceInflationFactorSchema.values()) {
            outputFields.add(
                    Schema.Field.of(schema.getName(), Schema.nullableOf(Schema.of(getSchemaType(schema.getType())))));
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".vif", outputFields);
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
        Schema inputSchema = getInputSchema(javaRDD);
        final List<Schema.Field> inputField = inputSchema.getFields();
        Map<String, Integer> dataTypeCountMap = getDataTypeCountMap(inputField);
        MultivariateStatisticalSummary summary = null;
        Map<String, Double> computedFeaturesMean = new HashMap<>();
        Map<String, Double> computedFeaturesStddev = new HashMap<>();
        JavaRDD<List<Tuple2<String, Double>>> tupledRDD = null;
        if (dataTypeCountMap.get("numeric") > 0) {
            JavaRDD<Vector> vectoredRDD = getVectorRDDForStats(javaRDD, inputField);
            summary = Statistics.colStats(vectoredRDD.rdd());
            if (summary.mean() == null || summary.variance() == null) {
                LOG.error("Computed mean and variance statistic on input data is not available");
                throw new IllegalStateException("Computed mean and variance statistic on input data is not available");
            }
            getComputedFeaturesStats(inputField, summary.mean().toArray(), summary.variance().toArray(),
                    computedFeaturesMean, computedFeaturesStddev);
            Broadcast<Map<String, Double>> featureMeanMap = sparkExecutionPluginContext.getSparkContext()
                    .broadcast(computedFeaturesMean);
            Broadcast<Map<String, Double>> featureSTDMap = sparkExecutionPluginContext.getSparkContext()
                    .broadcast(computedFeaturesStddev);
            tupledRDD = getFilteredStandardizedRDD(javaRDD, inputField, featureMeanMap, featureSTDMap);
            tupledRDD.cache();
            vectoredRDD = getVectorRDDForMean(tupledRDD, inputField);
            summary = Statistics.colStats(vectoredRDD.rdd());
            if (summary.mean() == null) {
                LOG.error("Computed mean statistic on input data is not available");
                throw new IllegalStateException("Computed mean statistic on input data is not available");
            }
            computedFeaturesMean = getComputedFeaturesMean(inputField, summary.mean().toArray(),
                    computedFeaturesStddev);
        } else {
            return sparkExecutionPluginContext.getSparkContext().parallelize(new LinkedList<StructuredRecord>());
        }
        
        List<StructuredRecord> recordList = computeVIFScoresAllIterations(computedFeaturesMean, tupledRDD, inputSchema);
        tupledRDD.unpersist();
        return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
    }
    
    private Map<String, Double> getComputedFeaturesMean(List<Field> inputField, double[] meanValue,
            Map<String, Double> computedFeaturesStddev) {
        Map<String, Double> computedFeaturesMean = new HashMap<>();
        int index = 0;
        for (Field field : inputField) {
            if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                Double stdDev = computedFeaturesStddev.get(field.getName());
                if (!stdDev.equals(ZERO)) {
                    computedFeaturesMean.put(field.getName(), meanValue[index++]);
                }
            }
        }
        return computedFeaturesMean;
    }
    
    private void getComputedFeaturesStats(List<Field> inputField, double[] meanValue, double[] variance,
            Map<String, Double> computedFeaturesMean, Map<String, Double> computedFeaturesStddev) {
        int index = 0;
        for (Field field : inputField) {
            if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                computedFeaturesMean.put(field.getName(), meanValue[index]);
                computedFeaturesStddev.put(field.getName(), Math.sqrt(variance[index++]));
            }
        }
    }
    
    private List<StructuredRecord> computeVIFScoresAllIterations(Map<String, Double> computedFeaturesMean,
            JavaRDD<List<Tuple2<String, Double>>> tupledRDD, Schema inputSchema) {
        Map<String, Double> computedRSquareMap = new HashMap<String, Double>();
        int numIterations = computedFeaturesMean.size();
        List<StructuredRecord> recordList = new LinkedList<>();
        long recordIndex = 1;
        for (int it = 0; it < numIterations && it < this.linearRegressionIterations; it++) {
            double maxVIF = Double.MIN_VALUE;
            String maxVIFFeature = "";
            Map<String, Double> featureVIFMap = new HashMap<String, Double>();
            for (Map.Entry<String, Double> targetVariable : computedFeaturesMean.entrySet()) {
                JavaRDD<LabeledPoint> labeledData = getLabeledRDD(targetVariable.getKey(), tupledRDD,
                        new HashSet<String>(computedRSquareMap.keySet()));
                LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(labeledData),
                        this.numIterations, this.stepSize);
                double rSquare = computeRSquare(labeledData, model, targetVariable.getValue());
                double vif = 1.0 / (1.0 - rSquare);
                if (vif > maxVIF) {
                    maxVIF = vif;
                    maxVIFFeature = targetVariable.getKey();
                }
                featureVIFMap.put(targetVariable.getKey(), vif);
            }
            computedRSquareMap.put(maxVIFFeature, maxVIF);
            computedFeaturesMean.remove(maxVIFFeature);
            StructuredRecord record = generateRecord(recordIndex++, maxVIFFeature, maxVIF, featureVIFMap, inputSchema);
            recordList.add(record);
            if (maxVIF < this.selectionThreshold) {
                break;
            }
        }
        return recordList;
    }
    
    private double computeRSquare(JavaRDD<LabeledPoint> labeledData, LinearRegressionModel model, Double actualYMean) {
        JavaPairRDD<Double, Double> valuesAndPreds = labeledData
                .mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint point) throws Exception {
                        return new Tuple2<Double, Double>(model.predict(point.features()), point.label());
                    }
                });
        double ssTotal = computeSSTotal(valuesAndPreds, actualYMean);
        double ssRes = computeSSRes(valuesAndPreds);
        return 1 - (ssRes / ssTotal);
    }
    
    private double computeSSTotal(JavaPairRDD<Double, Double> valuesAndPreds, Double actualYMean) {
        return valuesAndPreds.mapToDouble(new DoubleFunction<Tuple2<Double, Double>>() {
            
            @Override
            public double call(Tuple2<Double, Double> pair) throws Exception {
                double diff = pair._2 - actualYMean;
                return diff * diff;
            }
        }).sum();
    }
    
    private double computeSSRes(JavaPairRDD<Double, Double> valuesAndPreds) {
        return valuesAndPreds.mapToDouble(new DoubleFunction<Tuple2<Double, Double>>() {
            
            @Override
            public double call(Tuple2<Double, Double> pair) throws Exception {
                double diff = pair._1 - pair._2;
                return diff * diff;
            }
        }).sum();
    }
    
    private JavaRDD<LabeledPoint> getLabeledRDD(String targetFeature, JavaRDD<List<Tuple2<String, Double>>> tupledRDD,
            Set<String> processedFeatureSet) {
        return tupledRDD.map(new Function<List<Tuple2<String, Double>>, LabeledPoint>() {
            
            @Override
            public LabeledPoint call(List<Tuple2<String, Double>> input) throws Exception {
                List<Double> valueList = new LinkedList<Double>();
                double targetValue = 0;
                for (Tuple2<String, Double> tuple : input) {
                    if (processedFeatureSet.contains(tuple._1)) {
                        continue;
                    } else if (tuple._1.equals(targetFeature)) {
                        if (tuple._2 == null) {
                            targetValue = 0.0;
                        } else {
                            targetValue = tuple._2;
                        }
                    } else {
                        valueList.add(tuple._2);
                    }
                }
                double[] values = new double[valueList.size()];
                int index = 0;
                for (Double val : valueList) {
                    if (val == null) {
                        val = 0.0;
                    }
                    values[index++] = val;
                }
                return new LabeledPoint(targetValue, Vectors.dense(values));
            }
        });
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
    
    private StructuredRecord generateRecord(long recordIndex, String maxVIFFeature, double maxVIF,
            Map<String, Double> featureVIFMap, Schema inputSchema) {
        Schema schema = getOutputSchema(inputSchema);
        Builder builder = StructuredRecord.builder(schema);
        builder.set(VarianceInflationFactorSchema.ID.getName(), recordIndex);
        builder.set(VarianceInflationFactorSchema.MAX_VALUE_FEATURE_NAME.getName(), maxVIFFeature);
        builder.set(VarianceInflationFactorSchema.VIF_SCORE.getName(), maxVIF);
        builder.set(VarianceInflationFactorSchema.ALL_VIF_SCORES.getName(), GSON_OBJ.toJson(featureVIFMap));
        return builder.build();
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
    
    private JavaRDD<Vector> getVectorRDDForStats(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {
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
    
    private JavaRDD<Vector> getVectorRDDForMean(JavaRDD<List<Tuple2<String, Double>>> tupledRDD,
            List<Field> inputField) {
        return tupledRDD.map(new Function<List<Tuple2<String, Double>>, Vector>() {
            
            @Override
            public Vector call(List<Tuple2<String, Double>> input) throws Exception {
                int index = 0;
                double[] valDouble = new double[input.size()];
                for (Tuple2<String, Double> tuple : input) {
                    valDouble[index++] = tuple._2;
                }
                return Vectors.dense(valDouble);
            }
            
        });
    }
    
    private JavaRDD<List<Tuple2<String, Double>>> getFilteredStandardizedRDD(JavaRDD<StructuredRecord> javaRDD,
            List<Field> inputField, Broadcast<Map<String, Double>> featureMeanMapBroadcast,
            Broadcast<Map<String, Double>> featureSTDMapBroadcast) {
        return javaRDD.map(new Function<StructuredRecord, List<Tuple2<String, Double>>>() {
            
            @Override
            public List<Tuple2<String, Double>> call(StructuredRecord record) throws Exception {
                List<Tuple2<String, Double>> tupleList = new LinkedList<>();
                Map<String, Double> featureMeanMap = featureMeanMapBroadcast.getValue();
                Map<String, Double> featureSTDMap = featureSTDMapBroadcast.getValue();
                for (Schema.Field field : inputField) {
                    if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                        Object val = record.get(field.getName());
                        Double mean = featureMeanMap.get(field.getName());
                        Double stdev = featureSTDMap.get(field.getName());
                        if (stdev.equals(ZERO)) {
                            continue;
                        }
                        if (val == null) {
                            tupleList.add(new Tuple2<String, Double>(field.getName(), standardize(0.0, mean, stdev)));
                            continue;
                        }
                        if (getSchemaType(field.getSchema()).equals(Schema.Type.BOOLEAN)) {
                            double doubleVal = val.toString().equals("true") ? 1.0 : 0.0;
                            tupleList.add(
                                    new Tuple2<String, Double>(field.getName(), standardize(doubleVal, mean, stdev)));
                            continue;
                        }
                        try {
                            double doubleVal = Double.parseDouble(val.toString());
                            tupleList.add(
                                    new Tuple2<String, Double>(field.getName(), standardize(doubleVal, mean, stdev)));
                        } catch (Exception e) {
                            tupleList.add(new Tuple2<String, Double>(field.getName(), standardize(0.0, mean, stdev)));
                        }
                    }
                }
                return tupleList;
            }
            
            private double standardize(double value, double mean, double stdev) {
                return (value - mean) / stdev;
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
