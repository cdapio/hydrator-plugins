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
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon a label in the structured record using Decision Tree Regression.
 * Writes this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(DecisionTreeTrainer.PLUGIN_NAME)
@Description("Trains a regression model based upon a particular label and features of a record.")
public class DecisionTreeTrainer extends SparkMLTrainer {
  public static final String PLUGIN_NAME = "DecisionTreeTrainer";
  //Impurity measure of the homogeneity of the labels at the node. Expected value for regression is "variance".
  private static final String IMPURITY = "variance";
  private DecisionTreeTrainerConfig config;

  public DecisionTreeTrainer(DecisionTreeTrainerConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void trainModel(SparkContext context, Schema schema, JavaRDD<LabeledPoint> trainingData, String outputPath) {
    Map<Integer, Integer> categoricalFeaturesInfo =
      SparkUtils.getCategoricalFeatureInfo(schema, config.featureFieldsToInclude, config.featureFieldsToExclude,
                                           config.labelField, config.cardinalityMapping);
    final DecisionTreeModel model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, IMPURITY,
                                                                config.maxDepth, config.maxBins);
    model.save(context, outputPath);
  }

  /**
   * Configuration for DecisionTreeTrainer.
   */
  public static class DecisionTreeTrainerConfig extends MLTrainerConfig {

    @Nullable
    @Description("Mapping of the feature to the cardinality of that feature; required for categorical features.")
    private String cardinalityMapping;

    @Nullable
    @Description("Maximum depth of the tree.\n For example, depth 0 means 1 leaf node; depth 1 means 1 internal node " +
      "+ 2 leaf nodes. Default is 10.")
    private Integer maxDepth;

    @Nullable
    @Description("Maximum number of bins used for splitting when discretizing continuous features. " +
      "DecisionTree requires maxBins to be at least as large as the number of values in each categorical feature. " +
      "Default is 100.")
    private Integer maxBins;

    public DecisionTreeTrainerConfig() {
      super();
      maxDepth = 10;
      maxBins = 100;
    }

    public DecisionTreeTrainerConfig(String fileSetName, @Nullable String path, @Nullable String featuresToInclude,
                                     @Nullable String featuresToExclude, @Nullable String cardinalityMapping,
                                     String labelField, @Nullable Integer maxDepth, @Nullable Integer maxBins) {
      super(fileSetName, path, featuresToInclude, featuresToExclude, labelField);
      this.cardinalityMapping = cardinalityMapping;
      this.maxDepth = maxDepth;
      this.maxBins = maxBins;
    }
  }
}
