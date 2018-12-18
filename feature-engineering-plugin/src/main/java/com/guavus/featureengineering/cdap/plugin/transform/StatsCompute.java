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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Path;

/*TODO:This implementation is unoptimized and doesn't take advantage of spark for computing string stats and 
 * percentiles. Need to modify this implementation via adding similar logic of taking matrix transposes in spark.
 * Have a look at https://recalll.co/ask/v/topic/scala-How-to-transpose-an-RDD-in-Spark/557a43a52bd273720d8b892f
 * */

/**
 * SparkCompute plugin that generates different stats for given schema.
 */
public class StatsCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
    /**
     * 
     */
    private static final long serialVersionUID = 8592939878395242673L;
    public static final String NAME = "StatsCompute";
    private final Conf config;
    private int numberOfThreads;
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

    public StatsCompute(Conf config) {
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
        outputFields.add(Schema.Field.of("Statistic", Schema.of(Schema.Type.STRING)));
        for (Schema.Field field : inputSchema.getFields()) {

            outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

        }
        return Schema.recordOf(inputSchema.getRecordName() + ".stats", outputFields);
    }

    private List<List<Double>> getPercentileStatsRDD(List<List<Double>> doubleListRDD,
            SparkExecutionPluginContext sparkExecutionPluginContext, int length, long size)
            throws InterruptedException, ExecutionException {
        List<List<Double>> percentileValues = new LinkedList<List<Double>>();
        for (int i = 0; i < percentiles.length; i++) {
            percentileValues.add(new LinkedList<Double>());
        }

        for (int i = 0; i < doubleListRDD.get(0).size(); i++) {
            List<Double> valList = new ArrayList<Double>();
            for (int j = 0; j < doubleListRDD.size(); j++) {
                valList.add(doubleListRDD.get(j).get(i));
            }
            Collections.sort(valList);
            for (int j = 0; j < percentiles.length; j++) {
                double percentile = percentiles[j];
                int id = (int) (size * percentile);
                percentileValues.get(j).add(valList.get(id));
            }
        }

        return percentileValues;
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
        JavaRDD<Vector> vectoredRDD = getVectorRDD(javaRDD, inputField);
        MultivariateStatisticalSummary summary = Statistics.colStats(vectoredRDD.rdd());

        JavaRDD<List<String>> stringListRDD = getStringListRDD(javaRDD, inputField);

        List<List<Long>> stringStats = getStringStats(stringListRDD.collect(), inputField);
        stringListRDD.unpersist();

        JavaRDD<List<Double>> doubleListRDD = getDoubleListRDD(javaRDD, inputField);
        List<List<Double>> percentileScores = getPercentileStatsRDD(doubleListRDD.collect(),
                sparkExecutionPluginContext, summary.variance().toArray().length, size);
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

    private List<List<Long>> getStringStats(List<List<String>> stringListRDD, List<Field> inputField)
            throws InterruptedException, ExecutionException {
        List<List<Long>> stringStats = new LinkedList<>(); // 0 = totalCount, 1 = uniqueCount, 2 = top word, 3 = top
                                                           // word count
        for (int i = 0; i < 4; i++) {
            stringStats.add(new LinkedList<Long>());
        }

        for (int i = 0; i < stringListRDD.get(0).size(); i++) {
            Map<String, Long> wordCountMap = new HashMap<>();
            for (int j = 0; j < stringListRDD.size(); j++) {
                String key = stringListRDD.get(j).get(i);
                Long val = wordCountMap.get(key);
                if (val == null) {
                    wordCountMap.put(key, 1L);
                } else {
                    wordCountMap.put(key, val + 1);
                }
            }
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
