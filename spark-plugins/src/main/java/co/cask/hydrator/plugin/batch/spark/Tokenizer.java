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

package co.cask.hydrator.plugin.batch.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Tokenizer-SparkCompute that breaks text(such as sentence) into individual terms(usually words)
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(Tokenizer.PLUGIN_NAME)
@Description("Used to tokenize text(such as sentence) into individual terms(usually words)")
public class Tokenizer extends SparkCompute<StructuredRecord, StructuredRecord> {

    public static final String PLUGIN_NAME = "Tokenizer";
    private Config config;
    public Schema outputSchema;

    public Tokenizer(Config config) {
        this.config = config;
    }

    /**
     * Configuration for the Tokenizer Plugin.
     */
    public static class Config extends PluginConfig {
        @Description("Column on which tokenization is to be done")
        private final String columnToBeTokenized;

        @Description("Delimiter for tokenization")
        private final String delimiter;

        @Description("Output column name for tokenized data")
        private final String outputColumn;

        public Config(String outputColumn, String columnToBeTokenized, String delimiter) {
            this.columnToBeTokenized = columnToBeTokenized;
            this.delimiter = delimiter;
            this.outputColumn = outputColumn;
        }
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        if (inputSchema != null) {
            Schema.Type columnToBeTokenizedType = inputSchema.getField(config.columnToBeTokenized)
                    .getSchema().getType();
            Preconditions.checkArgument(columnToBeTokenizedType == Schema.Type.STRING,
                    "Column ,to be tokenized, must be of type String, but was %s.", columnToBeTokenizedType);
        }
    }

    @Override
    public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                               JavaRDD<StructuredRecord> input) throws Exception {
        JavaSparkContext javaSparkContext = context.getSparkContext();
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        //Create outputschema
        outputSchema = Schema.recordOf("outputSchema", Schema.Field.of(config.outputColumn,
                Schema.arrayOf(Schema.of(Schema.Type.STRING))));
        //Schema to be used to create dataframe
        StructType schema = new StructType(new StructField[]{
                new StructField(config.columnToBeTokenized, DataTypes.StringType, false, Metadata.empty())});
        //Transform input i.e JavaRDD<StructuredRecord> to JavaRDD<Row>
        JavaRDD<Row> rowRDD = input.map(new Function<StructuredRecord, Row>() {
            @Override
            public Row call(StructuredRecord rec) throws Exception {
                return RowFactory.create(rec.get(config.columnToBeTokenized));
            }
        });
        DataFrame sentenceDataFrame = sqlContext.createDataFrame(rowRDD, schema);
        RegexTokenizer tokenizer = new RegexTokenizer().setInputCol(config.columnToBeTokenized)
                .setOutputCol(config.outputColumn).setPattern(config.delimiter);
        DataFrame tokenizedDataFrame = tokenizer.transform(sentenceDataFrame);
        JavaRDD<Row> tokenizedRDD = javaSparkContext.parallelize(tokenizedDataFrame.
                select(config.outputColumn).collectAsList());
        //Transform JavaRDD<Row> to JavaRDD<StructuredRecord>
        JavaRDD<StructuredRecord> output = tokenizedRDD.map(new Function<Row, StructuredRecord>() {
            @Override
            public StructuredRecord call(Row row) throws Exception {
                StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
                for (Schema.Field field : outputSchema.getFields()) {
                    builder.set(field.getName(), row.getList(0));
                }
                return builder.build();
            }
        });
        return output;
    }
}
