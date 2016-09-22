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
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * SparkCompute that uses a trained Logistic Regression model to classify and tag input records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(LogisticRegressionClassifier.PLUGIN_NAME)
@Description("Uses a Logistic Regression model to classify records.")
public class LogisticRegressionClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "LogisticRegressionClassifier";
  private Config config;
  private Schema outputSchema;

  private LogisticRegressionModel loadedModel = null;
  private HashingTF tf = null;

  public LogisticRegressionClassifier(Config config) {
    this.config = config;
  }

  /**
   * Configuration for the LogisticRegressionClassifier.
   */
  public static class Config extends MLPredictorConfig {
    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer and equal to the" +
                  " number of features used in LogisticRegressionTrainer. The default value if none is provided " +
                  " will be 100.")
    private Integer numFeatures;

    public Config() {
      this.numFeatures = 100;
    }

    public Config(String fileSetName, String path, @Nullable String featureFieldsToInclude,
                  @Nullable String featureFieldsToExclude,
                  String predictionField, @Nullable Integer numFeatures) {
      super(fileSetName, path, featureFieldsToInclude, featureFieldsToExclude, predictionField);
      this.numFeatures = numFeatures;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validateConfigParameters(inputSchema, config.featureFieldsToInclude, config.featureFieldsToExclude,
                                    config.predictionField, null);

    outputSchema = SparkUtils.getOutputSchema(inputSchema, config.predictionField);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException("Failed to find model to use for classification." +
                                           " Location does not exist: " + modelLocation, null);
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);

    String modelPath = modelLocation.toURI().getPath();
    loadedModel = LogisticRegressionModel.load(sparkContext, modelPath);
    tf = new HashingTF((config.numFeatures));
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (!input.isEmpty()) {
      final Schema inputSchema = input.first().getSchema();
      final Map<String, Integer> featuresList = SparkUtils.getFeatureList(inputSchema,
                                                                          config.featureFieldsToInclude,
                                                                          config.featureFieldsToExclude,
                                                                          config.predictionField);

      final JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
        @Override
        public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
          List<Object> result = new ArrayList<>();
          for (String column : featuresList.keySet()) {
            if (structuredRecord.get(column) != null) {
              result.add(structuredRecord.get(column));
            }
          }
          double prediction = loadedModel.predict(tf.transform(result));

          outputSchema = (outputSchema != null) ? outputSchema :
            SparkUtils.getOutputSchema(inputSchema, config.predictionField);

          return SparkUtils.cloneRecord(structuredRecord, outputSchema, config.predictionField).
            set(config.predictionField, prediction).build();
        }
      });
      return output;
    }
    return context.getSparkContext().emptyRDD();
  }
}
