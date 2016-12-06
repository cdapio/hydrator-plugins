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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * NGramTransform - SparkCompute to transform input features into n-grams.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(NGramTransform.PLUGIN_NAME)
@Description("Used to transform input features into n-grams.")
public class NGramTransform extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "NGramTransform";
  private static final String TOKENIZED_FIELD = "intermediateTokens";
  private Schema outputSchema;
  private Config config;
  private NGram ngramTransformer;

  public NGramTransform(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
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
        StructuredRecord.Builder builder = StructuredRecord.builder(intermediateSchema);
        for (Schema.Field field : structuredRecord.getSchema().getFields()) {
          String fieldName = field.getName();
          builder.set(fieldName, structuredRecord.get(fieldName));
        }
        List<String> text = config.getInputFieldValue(structuredRecord);
        builder.set(TOKENIZED_FIELD, text);
        return StructuredRecordStringConverter.toJsonString(builder.build());
      }
    });
    DataFrame wordDataFrame = sqlContext.read().json(javardd);
    DataFrame ngramDataFrame = ngramTransformer.transform(wordDataFrame);
    JavaRDD<StructuredRecord> output = ngramDataFrame.toJSON().toJavaRDD()
      .map(new Function<String, StructuredRecord>() {
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

    @Description("Field to identify the entity to be tokenized. Can be of type words or characters.")
    private String tokenizationUnit;

    @Description("N-Gram size.")
    @Macro
    @Nullable
    private Integer ngramSize;

    @Description("Transformed field for sequence of n-gram.")
    private String outputField;

    public Config() {
      ngramSize = 1;
    }

    public Config(String fieldToBeTransformed, String tokenizationUnit, Integer ngramSize, String outputField) {
      this.fieldToBeTransformed = fieldToBeTransformed;
      this.tokenizationUnit = tokenizationUnit;
      this.ngramSize = ngramSize;
      this.outputField = outputField;
    }

    public void validate(Schema inputSchema) {
      if (inputSchema != null) {
        Schema schema = inputSchema.getField(fieldToBeTransformed).getSchema();
        Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
        if (type != Schema.Type.STRING) {
          throw new IllegalArgumentException(
            String.format("Field to be transformed should be of type string or nullable string. But was %s for field " +
                            "%s.", type, fieldToBeTransformed));
        }
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

    private List<String> getInputFieldValue(StructuredRecord record) {
      String field = record.get(fieldToBeTransformed);
      if (!Strings.isNullOrEmpty(field)) {
        if (tokenizationUnit.equalsIgnoreCase("word")) {
          return Lists.newArrayList(field.split("\\s+"));
        } else if (tokenizationUnit.equalsIgnoreCase("character")) {
          return Lists.newArrayList(field.split("(?<!^)"));
        } else {
          throw new IllegalArgumentException(String.format("Tokenization unit can accept only 2 values : words and " +
                                                             "characters. But was found to be : %s", tokenizationUnit));
        }
      } else {
        return new ArrayList<>();
      }
    }
  }
}
