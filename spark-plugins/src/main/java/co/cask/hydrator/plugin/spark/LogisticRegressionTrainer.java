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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkSink;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;

import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon various labels in the structured record.
 * Writes this model to a file of a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(LogisticRegressionTrainer.PLUGIN_NAME)
@Description("Uses Logistic Regression to train a model based upon whether messages are spam or not.")
public class LogisticRegressionTrainer extends SparkMLTrainer {
  public static final String PLUGIN_NAME = "LogisticRegressionTrainer";

  private Config config;

  /**
   * Configuration for the LogisticRegressionTrainer.
   */
  public static class Config extends MLTrainerConfig {

    @Nullable
    @Description("The number of classes to use in training the model. It must be of type integer. " +
      "The default value if none is provided will be 2.")
    private final Integer numClasses;

    public Config() {
      super();
      this.numClasses = 2;
    }

    public Config(String fileSetName, String path, @Nullable String featureFieldsToInclude, String labelField,
                  @Nullable String featureFieldsToExclude, @Nullable Integer numClasses) {
      super(fileSetName, path, featureFieldsToInclude, featureFieldsToExclude, labelField);
      this.numClasses = numClasses;
    }
  }

  public LogisticRegressionTrainer(Config config) {
    super(config);
    this.config = config;
  }

  public void trainModel(SparkContext context, Schema inputschema, JavaRDD<LabeledPoint> trainingData,
                         String modelPath) {
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(config.numClasses)
      .setIntercept(true)
      .run(trainingData.rdd());

    // save the model to a file in the output FileSet
    model.save(context, modelPath);
  }
}
