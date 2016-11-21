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
package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Tokenizer-SparkCompute that breaks text(such as sentence) into individual terms(usually words)
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(Tokenizer.PLUGIN_NAME)
@Description("Used to tokenize text(such as sentence) into individual terms(usually words)")
public class Tokenizer extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "Tokenizer";
  private Config config;
  private Schema outputSchema;

  public Tokenizer(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.columnToBeTokenized) != null) {
      Schema schema = inputSchema.getField(config.columnToBeTokenized).getSchema();
      Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
      Preconditions.checkArgument(type == Schema.Type.STRING, "Column to be tokenized %s must be of type String, " +
        "but was of type %s.", config.columnToBeTokenized, type);
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    if (input == null) {
      return context.getSparkContext().emptyRDD();
    }
    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(input.first().getSchema(),
                                                                                config.outputColumn);
    JavaRDD<String> javardd = input.map(new Function<StructuredRecord, String>() {
      @Override
      public String call(StructuredRecord structuredRecord) throws Exception {
        return StructuredRecordStringConverter.toJsonString(structuredRecord);
      }
    });
    DataFrame sentenceDataFrame = sqlContext.read().json(javardd);
    RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol(config.columnToBeTokenized)
      .setOutputCol(config.outputColumn)
      .setPattern(config.patternSeparator);
    DataFrame tokenizedDataFrame = regexTokenizer.transform(sentenceDataFrame);
    JavaRDD<StructuredRecord> output = tokenizedDataFrame.toJSON().toJavaRDD()
      .map(new Function<String, StructuredRecord>() {
        @Override
        public StructuredRecord call(String record) throws Exception {
          return StructuredRecordStringConverter.fromJsonString(record, outputSchema);
        }
      });
    return output;
  }

  /**
   * Configuration for the Tokenizer Plugin.
   */
  public static class Config extends PluginConfig {
    @Description("Column on which tokenization is to be done")
    private final String columnToBeTokenized;

    @Description("Pattern Separator")
    private final String patternSeparator;

    @Description("Output column name for tokenized data")
    private final String outputColumn;

    public Config(String outputColumn, String columnToBeTokenized, String patternSeparator) {
      this.columnToBeTokenized = columnToBeTokenized;
      this.patternSeparator = patternSeparator;
      this.outputColumn = outputColumn;
    }

    private Schema getOutputSchema(Schema inputSchema, String fieldName) {
      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      fields.add(Schema.Field.of(fieldName, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
      return Schema.recordOf("record", fields);
    }
  }
}
