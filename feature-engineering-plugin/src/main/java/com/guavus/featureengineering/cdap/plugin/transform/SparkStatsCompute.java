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
import co.cask.cdap.common.enums.FeatureSTATS;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * SparkCompute plugin that generates different stats for given schema.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(SparkStatsCompute.NAME)
@Description("Computes statistics for each schema column.")
public class SparkStatsCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
    /**
     * 
     */
    private static final long serialVersionUID = 3718869622401824253L;
    /**
     * 
     */
    public static final String NAME = "SparkStatsCompute";
    private final Conf config;
    private static final double percentiles[] = { 0.25, 0.5, 0.75 };

    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {

        @Nullable
        @Description("Dummy Field.")
        private String dummy;

        Conf() {
            this.dummy = "";
        }

    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }

    public SparkStatsCompute(Conf config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getVerticalFeaturesOutputSchema(request.inputSchema);
    }

    private Schema getVerticalFeaturesOutputSchema(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.add(Schema.Field.of("Id", Schema.of(Schema.Type.LONG)));
        for (FeatureSTATS stats : FeatureSTATS.values()) {
            outputFields.add(
                    Schema.Field.of(stats.getName(), Schema.nullableOf(Schema.of(getSchemaType(stats.getType())))));
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".stats", outputFields);
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

    private JavaRDD<List<Object>> getStringStats(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {

        return javaRDD.flatMapToPair(new PairFlatMapFunction<StructuredRecord, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(StructuredRecord record) throws Exception {
                List<Tuple2<String, String>> ret = new LinkedList<>();
                for (Schema.Field field : inputField) {
                    if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                        continue;
                    }
                    Object val = record.get(field.getName());
                    ret.add(new Tuple2<>(field.getName(), String.valueOf(val)));
                }
                return ret.iterator();
            }
        }).aggregateByKey(new HashMap<String, Long>(), new Function2<Map<String, Long>, String, Map<String, Long>>() {

            @Override
            public Map<String, Long> call(Map<String, Long> countMap, String value) throws Exception {
                if (countMap.containsKey(value)) {
                    Long count = countMap.get(value);
                    if (count == null) {
                        count = 0L;
                    }
                    countMap.put(value, count + 1L);
                } else {
                    countMap.put(value, 1L);
                }
                return countMap;
            }
        }, new Function2<Map<String, Long>, Map<String, Long>, Map<String, Long>>() {

            @Override
            public Map<String, Long> call(Map<String, Long> countMap1, Map<String, Long> countMap2) throws Exception {
                for (Map.Entry<String, Long> entry : countMap2.entrySet()) {
                    if (countMap1.containsKey(entry.getKey())) {
                        Long count = countMap1.get(entry.getKey());
                        if (count == null) {
                            count = 0L;
                        }
                        countMap1.put(entry.getKey(), count + entry.getValue());
                    } else {
                        countMap1.put(entry.getKey(), entry.getValue());
                    }
                }
                return countMap1;
            }
        }).map(new Function<Tuple2<String, Map<String, Long>>, List<Object>>() {

            @Override
            public List<Object> call(Tuple2<String, Map<String, Long>> wordCountMapTuple) throws Exception {
                Map<String, Long> wordCountMap = wordCountMapTuple._2;
                String featureName = wordCountMapTuple._1;
                List<Object> stringStats = new ArrayList<Object>(5);
                long noOfUniqueWords = wordCountMap.size();
                long mostFrequentWordCount = 0;
                long leastFrequentWordCount = Long.MAX_VALUE;
                long totalCount = 0;
                String maxOccuringWord = "";
                for (Map.Entry<String, Long> entry : wordCountMap.entrySet()) {
                    if (entry.getValue() > mostFrequentWordCount) {
                        mostFrequentWordCount = entry.getValue();
                        maxOccuringWord = entry.getKey();
                    }
                    leastFrequentWordCount = Math.min(leastFrequentWordCount, entry.getValue());
                    totalCount += entry.getValue();
                }
                stringStats.add(featureName);
                stringStats.add(totalCount);
                stringStats.add(noOfUniqueWords);
                stringStats.add(leastFrequentWordCount);
                stringStats.add(mostFrequentWordCount);
                stringStats.add(maxOccuringWord);

                return stringStats;
            }
        });
    }

    private JavaPairRDD<String, Long> getNullCounts(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {

        return javaRDD.flatMapToPair(new PairFlatMapFunction<StructuredRecord, String, Long>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(StructuredRecord record) throws Exception {
                List<Tuple2<String, Long>> ret = new LinkedList<>();
                for (Schema.Field field : inputField) {
                    Object val = record.get(field.getName());
                    Long count = 0L;
                    if (val == null) {
                        count = 1L;
                    }
                    ret.add(new Tuple2<>(field.getName(), count));
                }
                return ret.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {

            @Override
            public Long call(Long arg0, Long arg1) throws Exception {
                return arg0 + arg1;
            }
        });
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
        Map<String, List<Object>> percentileStatsMap = new HashMap<>();
        if (dataTypeCountMap.get("numeric") > 0) {
            JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
            summary = Statistics.colStats(vectoredRDD.rdd());
            JavaRDD<List<Object>> percentileStatsRDD = getPercentileStatsRDD(javaRDD, inputField);
            percentileStatsMap = getMapFromStatsRDD(percentileStatsRDD);
        }
        Map<String, List<Object>> stringStatsMap = new HashMap<>();
        if (dataTypeCountMap.get("string") > 0) {
            JavaRDD<List<Object>> stringStatsRDD = getStringStats(javaRDD, inputField);
            stringStatsMap = getMapFromStatsRDD(stringStatsRDD);
        }
        JavaPairRDD<String, Long> nullCountRDD = getNullCounts(javaRDD, inputField);

        List<StructuredRecord> recordList = createStructuredRecordWithVerticalSchema(summary, stringStatsMap,
                percentileStatsMap, nullCountRDD.collectAsMap(), inputSchema);
        return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
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

    private Map<String, List<Object>> getMapFromStatsRDD(JavaRDD<List<Object>> statsRDD) {
        List<List<Object>> statsList = statsRDD.collect();
        Map<String, List<Object>> statsMap = new HashMap<>();
        for (List<Object> stats : statsList) {
            statsMap.put((String) stats.get(0), stats);
        }
        return statsMap;
    }

    private List<StructuredRecord> createStructuredRecordWithVerticalSchema(MultivariateStatisticalSummary summary,
            Map<String, List<Object>> stringStatsMap, Map<String, List<Object>> percentileStatsMap,
            Map<String, Long> nullCountMap, Schema inputSchema) {
        List<StructuredRecord.Builder> builderList = new LinkedList<StructuredRecord.Builder>();
        Schema verticalSchema = getVerticalFeaturesOutputSchema(inputSchema);
        for (Schema.Field field : inputSchema.getFields()) {
            builderList.add(StructuredRecord.builder(verticalSchema));
        }
        addFeatureNameRecord(inputSchema, builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.variance().toArray(),
                FeatureSTATS.Variance.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.max().toArray(),
                FeatureSTATS.Max.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.min().toArray(),
                FeatureSTATS.Min.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.mean().toArray(),
                FeatureSTATS.Mean.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.numNonzeros().toArray(),
                FeatureSTATS.NumOfNonZeros.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.normL1().toArray(),
                FeatureSTATS.NormL1.getName(), builderList);
        addNumericStatsVertically(inputSchema, (summary == null) ? null : summary.normL2().toArray(),
                FeatureSTATS.NormL2.getName(), builderList);
        addPercentileStatsVertically(inputSchema, percentileStatsMap, builderList);
        addStringStatsVertically(inputSchema, stringStatsMap, builderList);
        addNullCountVertically(inputSchema, nullCountMap, builderList);

        List<StructuredRecord> recordList = new LinkedList<>();
        for (Builder builder : builderList) {
            recordList.add(builder.build());
        }
        return recordList;
    }

    private void addNullCountVertically(Schema inputSchema, Map<String, Long> nullCountMap, List<Builder> builderList) {
        int index = 0;
        for (Schema.Field field : inputSchema.getFields()) {
            Builder builder = builderList.get(index++);
            builder.set(FeatureSTATS.NumOfNulls.getName(), nullCountMap.get(field.getName()));
        }
    }

    private void addStringStatsVertically(Schema inputSchema, Map<String, List<Object>> stringStatsMap,
            List<Builder> builderList) {
        int index = 0;
        for (Schema.Field field : inputSchema.getFields()) {
            Builder builder = builderList.get(index++);
            if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                    || !stringStatsMap.containsKey(field.getName())) {
                builder.set(FeatureSTATS.TotalCount.getName(), null);
                builder.set(FeatureSTATS.UniqueCount.getName(), null);
                builder.set(FeatureSTATS.LeastFrequentWordCount.getName(), null);
                builder.set(FeatureSTATS.MostFrequentWordCount.getName(), null);
                builder.set(FeatureSTATS.MostFrequentEntry.getName(), null);
            } else {
                List<Object> stats = stringStatsMap.get(field.getName());
                builder.set(FeatureSTATS.TotalCount.getName(), (Long) stats.get(1));
                builder.set(FeatureSTATS.UniqueCount.getName(), (Long) stats.get(2));
                builder.set(FeatureSTATS.LeastFrequentWordCount.getName(), (Long) stats.get(3));
                builder.set(FeatureSTATS.MostFrequentWordCount.getName(), (Long) stats.get(4));
                builder.set(FeatureSTATS.MostFrequentEntry.getName(), (String) stats.get(5));
            }
        }
    }

    private void addPercentileStatsVertically(Schema inputSchema, Map<String, List<Object>> percentileStatsMap,
            List<Builder> builderList) {
        int index = 0;
        for (Schema.Field field : inputSchema.getFields()) {
            Builder builder = builderList.get(index++);
            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)
                    || !percentileStatsMap.containsKey(field.getName())) {
                builder.set(FeatureSTATS.TwentyFivePercentile.getName(), null);
                builder.set(FeatureSTATS.FiftyPercentile.getName(), null);
                builder.set(FeatureSTATS.SeventyFivePercentile.getName(), null);
                builder.set(FeatureSTATS.InterQuartilePercentile.getName(), null);
            } else {
                List<Object> stats = percentileStatsMap.get(field.getName());
                builder.set(FeatureSTATS.TwentyFivePercentile.getName(), (Double) stats.get(1));
                builder.set(FeatureSTATS.FiftyPercentile.getName(), (Double) stats.get(2));
                builder.set(FeatureSTATS.SeventyFivePercentile.getName(), (Double) stats.get(3));
                builder.set(FeatureSTATS.InterQuartilePercentile.getName(), (Double) stats.get(4));
            }
        }
    }

    private void addFeatureNameRecord(Schema inputSchema, List<Builder> builderList) {
        int index = 0;
        for (Schema.Field field : inputSchema.getFields()) {
            Builder builder = builderList.get(index++);
            builder.set(FeatureSTATS.Feature.getName(), field.getName());
            builder.set("Id", (long) index);
        }
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

    private void addNumericStatsVertically(Schema inputSchema, double[] values, String statName,
            List<Builder> builderList) {
        int index = 0;
        int valueIndex = 0;
        for (Schema.Field field : inputSchema.getFields()) {
            Builder builder = builderList.get(index++);
            if (getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                builder.set(statName, null);
            } else {
                if (values != null) {
                    builder.set(statName, values[valueIndex++]);
                } else {
                    builder.set(statName, null);
                }
            }
        }
    }

    private JavaRDD<List<Object>> getPercentileStatsRDD(JavaRDD<StructuredRecord> javaRDD,
            final List<Field> inputField) {

        return javaRDD.flatMapToPair(new PairFlatMapFunction<StructuredRecord, String, Double>() {

            @Override
            public Iterator<Tuple2<String, Double>> call(StructuredRecord record) throws Exception {
                List<Tuple2<String, Double>> ret = new LinkedList<>();
                List<Double> values = new LinkedList<Double>();
                for (Schema.Field field : inputField) {
                    if (!getSchemaType(field.getSchema()).equals(Schema.Type.STRING)) {
                        Object val = record.get(field.getName());
                        Double value = null;
                        if (val == null) {
                            value = 0.0;
                            continue;
                        }
                        if (getSchemaType(field.getSchema()).equals(Schema.Type.BOOLEAN)) {
                            val = val.toString().equals("true") ? 1 : 0;
                        }
                        try {
                            value = Double.parseDouble(val.toString());
                        } catch (Exception e) {
                            value = 0.0;
                        }
                        ret.add(new Tuple2<>(field.getName(), value));
                    }
                }
                return ret.iterator();
            }
        }).groupByKey().map(new Function<Tuple2<String, Iterable<Double>>, List<Object>>() {

            @Override
            public List<Object> call(Tuple2<String, Iterable<Double>> valueTuple) throws Exception {
                List<Object> resultStats = new ArrayList<Object>();
                List<Double> values = new ArrayList<Double>();
                Iterator<Double> it = valueTuple._2.iterator();
                while (it.hasNext()) {
                    values.add(it.next());
                }
                Collections.sort(values);

                resultStats.add(valueTuple._1); // adding feature name
                for (int i = 0; i < percentiles.length; i++) {
                    int id = (int) (values.size() * percentiles[i]);
                    resultStats.add(values.get(id));
                }
                resultStats.add((Double) resultStats.get(3) - (Double) resultStats.get(1));
                return resultStats;
            }

        });

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
