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
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Class to generate text based features using Hashing TF.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(HashingTFFeatureGenerator.PLUGIN_NAME)
@Description("SparkCompute to generate text based feature using Hashing TF technique.")
public class HashingTFFeatureGenerator extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "HashingTFFeatureGenerator";
  private HashingTFConfig config;
  private Schema outputSchema;
  private HashingTF hashingTF;

  @VisibleForTesting
  public HashingTFFeatureGenerator(HashingTFConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getOutputSchema(inputSchema));
    SparkUtils.validateFeatureGeneratorConfig(inputSchema, config.outputColumnMapping);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    hashingTF = new HashingTF(config.numFeatures);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {
    if (input.isEmpty()) {
      return context.getSparkContext().emptyRDD();
    }
    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(input.first().getSchema());
    final Map<String, String> mapping = config.getFeatureListMapping();

    return input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord input) throws Exception {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : input.getSchema().getFields()) {
          String fieldName = field.getName();
          builder.set(fieldName, input.get(fieldName));
        }
        for (Map.Entry<String, String> mapEntry : mapping.entrySet()) {
          String inputField = mapEntry.getKey();
          String outputField = mapEntry.getValue();
          List<String> text = SparkUtils.getInputFieldValue(input, inputField, config.pattern);
          Vector vector = text == null ? Vectors.sparse(0, new int[0], new double[0]) :
            hashingTF.transform(text);
          builder.set(outputField, VectorUtils.asRecord((SparseVector) vector));
        }
        return builder.build();
      }
    });
  }

  /**
   * Configuration for the HashingTFFeatureGenerator.
   */
  public static class HashingTFConfig extends PluginConfig {

    @Nullable
    @Description("Pattern to split the input string fields on. Default is '\\s+'.")
    private String pattern;

    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer. The default value " +
      "if none is provided will be 2^20.")
    private Integer numFeatures;

    @Description("A comma-separated list of the input fields to map to the transformed output fields. The key " +
      "specifies the name of the field to generate feature vector from, with its corresponding value specifying the " +
      "output columns(size, indices and value) to emit the sparse vector.")
    private String outputColumnMapping;

    public HashingTFConfig() {
      pattern = "\\s+";
      numFeatures = ((Double) Math.pow(2, 20)).intValue();
    }

    public HashingTFConfig(@Nullable String pattern, @Nullable Integer numFeatures, String outputColumnMapping) {
      this.pattern = pattern;
      this.numFeatures = numFeatures;
      this.outputColumnMapping = outputColumnMapping;
    }

    private Map<String, String> getFeatureListMapping() {
      try {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator(":").split(outputColumnMapping);
        return map;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid output feature mapping. %s. Please make sure it is in the format " +
                          "'input-column':'transformed-output-column'.", e.getMessage()), e);
      }
    }

    private Schema getOutputSchema(Schema inputSchema) {
      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      for (Map.Entry<String, String> entry : getFeatureListMapping().entrySet()) {
        fields.add(Schema.Field.of(entry.getValue(), Schema.recordOf(
          "sparseVector", Schema.Field.of("size", Schema.of(Schema.Type.INT)),
          Schema.Field.of("indices", Schema.arrayOf(Schema.of(Schema.Type.INT))),
          Schema.Field.of("vectorValues", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))))));
      }
      return Schema.recordOf("record", fields);
    }
  }
}
