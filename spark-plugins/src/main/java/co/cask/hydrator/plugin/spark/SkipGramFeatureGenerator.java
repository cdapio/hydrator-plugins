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
import co.cask.cdap.api.dataset.lib.FileSet;
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
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Class to generate text based features using SkipGram model (Spark's Word2Vec).
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(SkipGramFeatureGenerator.PLUGIN_NAME)
@Description("Generate text based features using SkipGram model (Spark's Word2Vec).")
public class SkipGramFeatureGenerator extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "SkipGramFeatureGenerator";
  private FeatureGeneratorConfig config;
  private Schema outputSchema;
  private Word2VecModel loadedModel;
  private Splitter splitter;
  private Pattern pattern;

  @VisibleForTesting
  public SkipGramFeatureGenerator(FeatureGeneratorConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    SparkUtils.validateFeatureGeneratorConfig(inputSchema, config.getFeatureListMapping(config.outputColumnMapping),
                                              config.pattern);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getOutputSchema(inputSchema));
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException(String.format("Failed to find model. Location does not exist: %s.",
                                                       modelLocation));
    }
    // load the model from a file in the model fileset
    loadedModel = Word2VecModel.load(context.getSparkContext().sc(), modelLocation.toURI().getPath());
    pattern = Pattern.compile(config.pattern);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (input.isEmpty()) {
      return context.getSparkContext().emptyRDD();
    }
    outputSchema = outputSchema != null ? outputSchema : config.getOutputSchema(input.first().getSchema());
    final Map<String, String> mapping = config.getFeatureListMapping(config.outputColumnMapping);

    final int vectorSize = loadedModel.wordVectors().length / loadedModel.wordIndex().size();
    return input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord input) throws Exception {
        splitter = splitter == null ? Splitter.on(pattern) : splitter;
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : input.getSchema().getFields()) {
          String fieldName = field.getName();
          builder.set(fieldName, input.get(fieldName));
        }
        for (Map.Entry<String, String> mapEntry : mapping.entrySet()) {
          String inputField = mapEntry.getKey();
          String outputField = mapEntry.getValue();
          List<String> text = SparkUtils.getInputFieldValue(input, inputField, splitter);
          int numWords = 0;
          double[] vectorValues = new double[vectorSize];
          for (String word : text) {
            double[] wordVector = loadedModel.transform(word).toArray();
            for (int i = 0; i < vectorSize; i++) {
              vectorValues[i] += wordVector[i];
            }
            numWords++;
          }
          for (int i = 0; i < vectorSize; i++) {
            vectorValues[i] /= (double) numWords;
          }
          builder.set(outputField, vectorValues);
        }
        return builder.build();
      }
    });
  }

  /**
   * Config class for SkipGramFeatureGenerator.
   */
  public static class FeatureGeneratorConfig extends PluginConfig {
    @Description("The name of the FileSet to load the model from.")
    private String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private String path;

    @Description("List of the input fields to map to the transformed output fields. The key specifies the name of " +
      "the field to generate feature vector from, with its corresponding value the output column in which the vector " +
      "would be emitted.")
    private String outputColumnMapping;

    @Nullable
    @Description("Pattern to split the input string fields on. Default is '\\s+'.")
    private String pattern;

    public FeatureGeneratorConfig() {
      pattern = "\\s+";
    }

    public FeatureGeneratorConfig(String fileSetName, String path, String outputColumnMapping, String pattern) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.outputColumnMapping = outputColumnMapping;
      this.pattern = pattern;
    }

    private Schema getOutputSchema(Schema inputSchema) {
      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      for (Map.Entry<String, String> entry : getFeatureListMapping(outputColumnMapping).entrySet()) {
        fields.add(Schema.Field.of(entry.getValue(), Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))));
      }
      return Schema.recordOf("record", fields);
    }

    /**
     * Get the input feature to output column mapping. Throws exception if it is not a valid mapping.
     *
     * @param outputColumnMapping input field to output field mapping as provided by user
     * @return map of input field to transformed output field names
     */
    private Map<String, String> getFeatureListMapping(String outputColumnMapping) {
      try {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator(":").split(outputColumnMapping);
        return map;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid output feature mapping %s. Please provide a valid output column mapping which can be" +
                          "used to store the generated vector as array of double. 'outputColumnMapping' should be in " +
                          "the format 'input-column':'transformed-output-column'. %s.",
                        outputColumnMapping, e.getMessage()), e);
      }
    }
  }
}
