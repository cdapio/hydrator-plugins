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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon various labels in the structured record.
 * Writes this model to a file of a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(LogisticRegressionTrainer.PLUGIN_NAME)
@Description("Uses Logistic Regression to train a model based upon whether messages are spam or not.")
public class LogisticRegressionTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "LogisticRegressionTrainer";

  private Config config;

  /**
   * Configuration for the LogisticRegressionTrainer.
   */
  public static class Config extends MLTrainerConfig {

    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer and equal to the" +
      " number of features used in LogisticRegressionClassifier. The default value if none is provided will be" +
      " 100.")
    private final Integer numFeatures;

    @Nullable
    @Description("The number of classes to use in training the model. It must be of type integer. " +
      "The default value if none is provided will be 2.")
    private final Integer numClasses;

    public Config() {
      this.numFeatures = 100;
      this.numClasses = 2;
    }

    public Config(String fileSetName, String path, @Nullable String featureFieldsToInclude, String labelField,
                  @Nullable String featureFieldsToExclude, @Nullable Integer numFeatures,
                  @Nullable Integer numClasses) {
      super(fileSetName, path, featureFieldsToInclude, featureFieldsToExclude, labelField);
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validateConfigParameters(inputSchema, config.featureFieldsToInclude, config.featureFieldsToExclude,
                                    config.labelField, null);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    final HashingTF tf = new HashingTF((config.numFeatures));

    if (!input.isEmpty()) {
      final Schema inputSchema = input.first().getSchema();

      final Map<String, Integer> featuresList = SparkUtils.getFeatureList(inputSchema,
                                                                          config.featureFieldsToInclude,
                                                                          config.featureFieldsToExclude,
                                                                          config.labelField);

      JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
        @Override
        public LabeledPoint call(StructuredRecord record) throws Exception {
          List<Object> result = new ArrayList<>();
          for (String column : featuresList.keySet()) {
            if (record.get(column) != null) {
              result.add(record.get(column));
            }
          }
          return new LabeledPoint((Double) record.get(config.labelField), tf.transform(result));
        }
      });

      trainingData.cache();

      final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
        .setNumClasses(config.numClasses)
        .run(trainingData.rdd());

      // save the model to a file in the output FileSet
      JavaSparkContext sparkContext = context.getSparkContext();
      FileSet outputFS = context.getDataset(config.fileSetName);
      model.save(JavaSparkContext.toSparkContext(sparkContext),
                 outputFS.getBaseLocation().append(config.path).toURI().getPath());
    }
  }
}
