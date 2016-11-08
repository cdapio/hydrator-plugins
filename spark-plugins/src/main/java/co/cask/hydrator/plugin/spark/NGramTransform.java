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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * NGramTransform - SparkCompute to transform input features into n-grams.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(NGramTransform.PLUGIN_NAME)
@Description("Used to transform input features into n-grams.")
public class NGramTransform extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "NGramTransform";
  public Schema outputSchema;
  private Config config;
  private Splitter splitter;
  private Pattern pattern;
  private NGram ngramTransformer;
  private static final String TOKENIZED_FIELD = "intermediateTokens";

  public NGramTransform(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    pattern = Pattern.compile(config.patternSeparator);
    ngramTransformer = new NGram().setN(config.ngramSize).setInputCol(TOKENIZED_FIELD)
      .setOutputCol(config.outputField);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    if (input.isEmpty()) {
      return context.getSparkContext().emptyRDD();
    }
    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(input.first().getSchema(),
                                                                                config.outputField);
    final Schema intermediateSchema = config.getOutputSchema(input.first().getSchema(), TOKENIZED_FIELD);

    JavaRDD<String> javardd = input.map(new Function<StructuredRecord, String>() {
      @Override
      public String call(StructuredRecord structuredRecord) throws Exception {
        splitter = splitter == null ? Splitter.on(pattern) : splitter;
        StructuredRecord.Builder builder = StructuredRecord.builder(intermediateSchema);
        for (Schema.Field field : structuredRecord.getSchema().getFields()) {
          String fieldName = field.getName();
          builder.set(fieldName, structuredRecord.get(fieldName));
        }
        List<String> text = SparkUtils.getInputFieldValue(structuredRecord, config.fieldToBeTransformed, splitter);
        builder.set(TOKENIZED_FIELD, text);
        return StructuredRecordStringConverter.toJsonString(builder.build());
      }
    });
    DataFrame wordDataFrame = sqlContext.read().json(javardd);
    DataFrame ngramDataFrame = ngramTransformer.transform(wordDataFrame);
   JavaRDD<StructuredRecord> output = ngramDataFrame.toJSON().toJavaRDD().map(new Function<String, StructuredRecord>() {
        @Override
        public StructuredRecord call(String record) throws Exception {
          return StructuredRecordStringConverter.fromJsonString(record, outputSchema);
        }
      });
    return output;
  }

  /**
   * Configuration for the NGramTransform Plugin.
   */
  public static class Config extends PluginConfig {

    @Description("Field to be used to transform input features into n-grams.")
    private String fieldToBeTransformed;

    @Nullable
    @Description("Pattern to split the input string fields on. Default is '\\s+'.")
    private String patternSeparator;

    @Description("N-Gram size.")
    @Macro
    @Nullable
    private Integer ngramSize;

    @Description("Transformed field for sequence of n-gram.")
    private String outputField;

    public Config() {
      patternSeparator = "\\s+";
      ngramSize = 1;
    }

    public Config(String fieldToBeTransformed, String patternSeparator, Integer ngramSize, String outputField) {
      this.fieldToBeTransformed = fieldToBeTransformed;
      this.patternSeparator = patternSeparator;
      this.ngramSize = ngramSize;
      this.outputField = outputField;
    }

    private void validate(Schema inputSchema) {
      if (inputSchema != null) {
        SparkUtils.validateTextField(inputSchema, fieldToBeTransformed);
      }
      if (ngramSize < 1) {
        throw new IllegalArgumentException(String.format("Minimum n-gram length required : 1. But found : %s.",
                                                         ngramSize));
      }
    }

    private Schema getOutputSchema(Schema inputSchema, String fieldName) {
      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      fields.add(Schema.Field.of(fieldName, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
      return Schema.recordOf("record", fields);
    }
  }
}
