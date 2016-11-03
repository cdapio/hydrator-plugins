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
import co.cask.cdap.etl.api.batch.SparkCompute;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;

/**
 * SparkCompute that uses a trained Logistic Regression model to classify and tag input records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(LogisticRegressionClassifier.PLUGIN_NAME)
@Description("Uses a Logistic Regression model to classify records.")
public class LogisticRegressionClassifier extends SparkMLPredictor {

  public static final String PLUGIN_NAME = "LogisticRegressionClassifier";
  private LogisticRegressionModel loadedModel;

  public LogisticRegressionClassifier(MLPredictorConfig config) {
    super(config);
  }

  @Override
  public void initialize(SparkContext context, String modelPath) {
    loadedModel = LogisticRegressionModel.load(context, modelPath);
  }

  @Override
  public double predict(Vector vector) {
    return loadedModel.predict(vector);
  }
}
