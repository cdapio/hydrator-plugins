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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.ws.rs.Path;

/**
 * SparkCompute plugin that counts how many times each word appears in records
 * input to the compute stage.
 */
public class StatsComputeToBeTested extends SparkCompute<StructuredRecord, StructuredRecord> {
    /**
     * 
     */
    private static final long serialVersionUID = 8592939878395242673L;
    public static final String NAME = "StatsCompute";
    private final Conf config;
    private int numberOfThreads;
    private int privateNumOfThreads;
    private static final double percentiles[] = { 0.25, 0.5, 0.75 };

    /**
     * Config properties for the plugin.
     */
    @VisibleForTesting
    public static class Conf extends PluginConfig {

        @Description("The field from the input records containing the words to count.")
        private String parallelThreads;

        Conf(String parallelThreads) {
            this.parallelThreads = parallelThreads;
        }

        Conf() {
            this.parallelThreads = "";
        }

        int getParallelThreads() {
            Iterable<String> parallelTh = Splitter.on(',').trimResults().split(parallelThreads);
            try {
                return Integer.parseInt(parallelTh.iterator().next());
            } catch (Exception e) {
                return 10;
            }
        }
    }

    @Override
    public void initialize(SparkExecutionPluginContext context) throws Exception {
        this.numberOfThreads = config.getParallelThreads();
        this.privateNumOfThreads = (numberOfThreads / 5);
    }

    /**
     * Endpoint request for output schema.
     */
    public static class GetSchemaRequest extends Conf {
        private Schema inputSchema;
    }

    private static class Identity<T> implements Function<T, T> {
        @Override
        public T call(T t) throws Exception {
            return t;
        }
    }

    private static class CountFunction implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, Long> {

        @Override
        public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<String>> tuples) throws Exception {
            String word = tuples._1();
            Long count = 0L;
            for (String s : tuples._2()) {
                count++;
            }
            List<Tuple2<String, Long>> output = new ArrayList<>();
            output.add(new Tuple2<>(word, count));
            return output.iterator();
        }
    }

    public StatsComputeToBeTested(Conf config) {
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
        Schema outputSchema = getOutputSchema(inputSchema);
        // set the output schema so downstream stages will know their input schema.
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

    @Path("outputSchema")
    public Schema getOutputSchema(GetSchemaRequest request) {
        return getOutputSchema(request.inputSchema);
    }

    private Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.add(Schema.Field.of("statistic", Schema.of(Schema.Type.STRING)));
        for (Schema.Field field : inputSchema.getFields()) {

            outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

        }
        return Schema.recordOf(inputSchema.getRecordName() + ".stats", outputFields);
    }

    private List<List<Double>> getPercentileStatsRDD(JavaRDD<List<Double>> doubleListRDD,
            SparkExecutionPluginContext sparkExecutionPluginContext, int length, long size)
            throws InterruptedException, ExecutionException {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadSize = length / this.privateNumOfThreads;
        List<List<Double>> percentileValues = new LinkedList<List<Double>>();
        for (int i = 0; i < percentiles.length; i++) {
            percentileValues.add(new LinkedList<Double>());
        }

        List<Future<List<List<Double>>>> futureList = new LinkedList<>();
        for (int ind = 0; ind < length; ind += threadSize) {
            Future<List<List<Double>>> future = threadPool.submit(new PercentileStatsCallable(ind, length,
                    ind + threadSize, doubleListRDD, size, this.numberOfThreads));
            futureList.add(future);
        }
        for (Future<List<List<Double>>> future : futureList) {
            try {
                List<List<Double>> tempPercentileValues = future.get();
                for (int i = 0; i < percentiles.length; i++) {
                    percentileValues.get(i).addAll(tempPercentileValues.get(i));
                }
            } catch (Throwable th) {
                throw new IllegalArgumentException(
                        "--------------------------- Got exception ---------------- with messsage = "
                                + th.getMessage());
            }
        }

        return percentileValues;
    }

    private static class PercentileStatsCallable implements Callable<List<List<Double>>>, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 6069451393213358352L;
        private int maxSize;
        private int minSize;
        private int curSize;
        private JavaRDD<List<Double>> doubleListRDD;
        private long rowSize;
        private int numberOfThreads;

        PercentileStatsCallable(int minSize, int maxSize, int curSize, JavaRDD<List<Double>> doubleListRDD,
                long rowSize, int numberOfThreads) {
            this.maxSize = maxSize;
            this.curSize = curSize;
            this.doubleListRDD = doubleListRDD;
            this.minSize = minSize;
            this.rowSize = rowSize;
            this.numberOfThreads = numberOfThreads;
        }

        @Override
        public List<List<Double>> call() throws Exception {

            List<List<Double>> percentileValues = new LinkedList<List<Double>>();
            for (int i = 0; i < percentiles.length; i++) {
                percentileValues.add(new LinkedList<Double>());
            }

            for (int i = minSize; i < maxSize && i < curSize; i++) {
                final int k = i;
                JavaRDD<Double> columnRDD = doubleListRDD.map(new Function<List<Double>, Double>() {

                    @Override
                    public Double call(List<Double> input) throws Exception {
                        return input.get(k);
                    }
                });

                JavaRDD<Double> sorted = columnRDD.sortBy(new Identity<Double>(), true, this.numberOfThreads);
                JavaPairRDD<Long, Double> indexed = sorted.zipWithIndex()
                        .mapToPair(new PairFunction<Tuple2<Double, Long>, Long, Double>() {

                            @Override
                            public Tuple2<Long, Double> call(Tuple2<Double, Long> t) throws Exception {
                                return t.swap();
                            }
                        });

                for (int j = 0; j < percentiles.length; j++) {
                    double percentile = percentiles[j];
                    long id = (long) (rowSize * percentile);
                    percentileValues.get(j).add(indexed.lookup(id).get(0));
                }
            }

            return percentileValues;
        }

    }

    private JavaRDD<List<String>> getStringListRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {

        return javaRDD.map(new Function<StructuredRecord, List<String>>() {

            @Override
            public List<String> call(StructuredRecord record) throws Exception {
                List<String> values = new LinkedList<String>();
                for (Schema.Field field : inputField) {
                    if (!field.getSchema().getType().equals(Schema.Type.STRING)) {
                        continue;
                    }
                    Object val = record.get(field.getName());
                    values.add(String.valueOf(val));
                }
                return values;
            }
        });

    }

    @Override
    public JavaRDD<StructuredRecord> transform(final SparkExecutionPluginContext sparkExecutionPluginContext,
            JavaRDD<StructuredRecord> javaRDD) throws Exception {
        final List<Schema.Field> inputField = sparkExecutionPluginContext.getInputSchema().getFields();
        Schema outputSchema = sparkExecutionPluginContext.getOutputSchema();
        long size = javaRDD.count();
        javaRDD.repartition(this.numberOfThreads);
        javaRDD.cache();
        JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
        vectoredRDD.cache();
        MultivariateStatisticalSummary summary = Statistics.colStats(vectoredRDD.rdd());
        vectoredRDD.unpersist();

        JavaRDD<List<String>> stringListRDD = getStringListRDD(javaRDD, inputField);
        stringListRDD.cache();
        List<List<Long>> stringStats = getStringStats(stringListRDD, inputField);
        stringListRDD.unpersist();

        JavaRDD<List<Double>> doubleListRDD = getDoubleListRDD(javaRDD, inputField);
        doubleListRDD.cache();
        javaRDD.unpersist();
        List<List<Double>> percentileScores = getPercentileStatsRDD(doubleListRDD, sparkExecutionPluginContext,
                summary.variance().toArray().length, size);
        doubleListRDD.unpersist();
        List<StructuredRecord> recordList = createStructuredRecord(summary, stringStats, percentileScores, outputSchema,
                sparkExecutionPluginContext.getInputSchema());

        return sparkExecutionPluginContext.getSparkContext().parallelize(recordList);
    }

    private List<StructuredRecord> createStructuredRecord(MultivariateStatisticalSummary summary,
            List<List<Long>> stringStats, List<List<Double>> percentileScores, Schema outputSchema,
            Schema inputSchema) {
        List<StructuredRecord> recordList = new LinkedList<StructuredRecord>();

        StructuredRecord record = addNumericStat(inputSchema, outputSchema, summary.variance().toArray(), "Variance");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.max().toArray(), "Max");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.min().toArray(), "Min");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.mean().toArray(), "Mean");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.numNonzeros().toArray(), "NumOfNonZeros");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.normL1().toArray(), "NormL1");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, summary.normL2().toArray(), "NormL2");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(0)),
                "25 Percentile");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(1)),
                "50 Percentile");
        recordList.add(record);

        record = addNumericStat(inputSchema, outputSchema, convertToPrimitiveDoubleArray(percentileScores.get(2)),
                "75 Percentile");
        recordList.add(record);

        record = addStringStat(inputSchema, outputSchema, stringStats.get(0), "TotalCount");
        recordList.add(record);
        record = addStringStat(inputSchema, outputSchema, stringStats.get(1), "UniqueCount");
        recordList.add(record);
        record = addStringStat(inputSchema, outputSchema, stringStats.get(2), "LeastFrequentWordCount");
        recordList.add(record);
        record = addStringStat(inputSchema, outputSchema, stringStats.get(3), "MostFrequentWordCount");
        recordList.add(record);

        return recordList;
    }

    private double[] convertToPrimitiveDoubleArray(List<Double> list) {
        double[] result = new double[list.size()];
        int index = 0;
        for (Double d : list) {
            result[index++] = d;
        }
        return result;
    }

    private StructuredRecord addStringStat(Schema inputSchema, Schema outputSchema, List<Long> values,
            String statName) {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

        int index = 0;
        int valueIndex = 0;
        List<Schema.Field> outputFields = outputSchema.getFields();
        List<Schema.Field> inputFields = inputSchema.getFields();
        builder.set(outputFields.get(index).getName(), statName);

        for (index = 0; index < inputFields.size(); index++) {
            Schema.Field field = inputFields.get(index);
            if (!field.getSchema().getType().equals(Schema.Type.STRING)) {
                builder.set(outputFields.get(index + 1).getName(), null);
            } else {
                builder.set(outputFields.get(index + 1).getName(), (values.get(valueIndex++) * 1.0));
            }
        }

        return builder.build();
    }

    private StructuredRecord addNumericStat(Schema inputSchema, Schema outputSchema, double[] values, String statName) {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

        int index = 0;
        int valueIndex = 0;
        List<Schema.Field> outputFields = outputSchema.getFields();
        builder.set(outputFields.get(index++).getName(), statName);

        for (Schema.Field field : inputSchema.getFields()) {
            if (field.getSchema().getType().equals(Schema.Type.STRING)) {
                builder.set(outputFields.get(index++).getName(), null);
                continue;
            }
            builder.set(outputFields.get(index++).getName(), values[valueIndex++]);
        }

        return builder.build();
    }

    private List<List<Long>> getStringStats(JavaRDD<List<String>> stringListRDD, List<Field> inputField)
            throws InterruptedException, ExecutionException {

        int length = inputField.size();
        length = 0;
        for (Schema.Field field : inputField) {
            if (field.getSchema().getType().equals(Schema.Type.STRING)) {
                length++;
            }
        }
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadSize = length / this.privateNumOfThreads;
        List<List<Long>> stringStats = new LinkedList<>(); // 0 = totalCount, 1 = uniqueCount, 2 = top word, 3 = top
                                                           // word count
        for (int i = 0; i < 4; i++) {
            stringStats.add(new LinkedList<Long>());
            for (int j = 0; j < length; j++) {
                stringStats.get(i).add(0L);
            }
        }
        List<Future<List<List<Long>>>> futureList = new LinkedList<>();
        for (int size = 0; size < length; size += threadSize) {
            Future<List<List<Long>>> future = threadPool
                    .submit(new StringStatsCallable(size, length, size + threadSize, stringListRDD));
            futureList.add(future);
        }
        for (Future<List<List<Long>>> future : futureList) {
            List<List<Long>> tempStringStats = future.get();
            for (int i = 0; i < 4; i++) {
                stringStats.get(i).addAll(tempStringStats.get(i));
            }
        }
        return stringStats;
    }

    private static class StringStatsCallable implements Callable<List<List<Long>>>, Serializable {
        /**
         * 
         */
        private static final long serialVersionUID = -2551214909334582731L;
        private int maxSize;
        private int minSize;
        private int curSize;
        JavaRDD<List<String>> stringListRDD;

        StringStatsCallable(int minSize, int maxSize, int curSize, JavaRDD<List<String>> stringListRDD) {
            this.maxSize = maxSize;
            this.curSize = curSize;
            this.stringListRDD = stringListRDD;
            this.minSize = minSize;
        }

        @Override
        public List<List<Long>> call() throws Exception {
            List<List<Long>> stringStats = new LinkedList<>(); // 0 = totalCount, 1 = uniqueCount, 2 = top word, 3 =
                                                               // top
            // word count
            for (int i = 0; i < 4; i++) {
                stringStats.add(new LinkedList<Long>());
            }
            for (int i = minSize; i < curSize && i < maxSize; i++) {
                final int k = i;
                JavaPairRDD<String, Long> wordCountPairRDD = stringListRDD.map(new Function<List<String>, String>() {

                    @Override
                    public String call(List<String> arg0) throws Exception {
                        return arg0.get(k);
                    }
                }).groupBy(new Identity<String>()).flatMapToPair(new CountFunction());

                Map<String, Long> wordCountMap = wordCountPairRDD.collectAsMap();
                long noOfUniqueWords = wordCountMap.size();
                long mostFrequentWordCount = 0;
                long leastFrequentWordCount = Long.MAX_VALUE;
                long totalCount = 0;
                for (Map.Entry<String, Long> entry : wordCountMap.entrySet()) {
                    mostFrequentWordCount = Math.max(entry.getValue(), mostFrequentWordCount);
                    leastFrequentWordCount = Math.min(leastFrequentWordCount, entry.getValue());
                    totalCount += entry.getValue();
                }
                stringStats.get(0).add(totalCount);
                stringStats.get(1).add(noOfUniqueWords);
                stringStats.get(2).add(leastFrequentWordCount);
                stringStats.get(3).add(mostFrequentWordCount);
            }
            return stringStats;
        }

    }

    private JavaRDD<List<Double>> getDoubleListRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {
        return javaRDD.map(new Function<StructuredRecord, List<Double>>() {

            @Override
            public List<Double> call(StructuredRecord record) throws Exception {
                List<Double> values = new LinkedList<Double>();
                for (Schema.Field field : inputField) {
                    if (!field.getSchema().getType().equals(Schema.Type.STRING)) {
                        Object val = record.get(field.getName());
                        if (val == null) {
                            values.add(0.0);
                            continue;
                        }
                        if (field.getSchema().getType().equals(Schema.Type.BOOLEAN)) {
                            val = val.toString().equals("true") ? 1 : 0;
                        }
                        try {
                            values.add(Double.parseDouble(val.toString()));
                        } catch (Exception e) {
                            values.add(0.0);
                        }
                    }
                }
                return values;
            }
        });
    }

    private JavaRDD<Vector> getVectorRDD(JavaRDD<StructuredRecord> javaRDD, final List<Field> inputField) {

        return javaRDD.map(new Function<StructuredRecord, Vector>() {

            @Override
            public Vector call(StructuredRecord record) throws Exception {
                List<Double> values = new LinkedList<Double>();
                for (Schema.Field field : inputField) {
                    if (!field.getSchema().getType().equals(Schema.Type.STRING)) {
                        Object val = record.get(field.getName());
                        if (val == null) {
                            values.add(0.0);
                            continue;
                        }
                        if (field.getSchema().getType().equals(Schema.Type.BOOLEAN)) {
                            val = val.toString().equals("true") ? 1 : 0;
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

}
