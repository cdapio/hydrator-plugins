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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon a label in the structured record using Decision Tree Regression.
 * Writes this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(DecisionTreeTrainer.PLUGIN_NAME)
@Description("Trains a regression model based upon a particular label and features of a record.")
public class DecisionTreeTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "DecisionTreeTrainer";
  //Impurity measure of the homogeneity of the labels at the node. Expected value for regression is "variance".
  private static final String IMPURITY = "variance";
  private DecisionTreeTrainerConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void run(final SparkExecutionPluginContext context, final JavaRDD<StructuredRecord> input) throws Exception {

    if (input == null) {
      return;
    }
    Schema schema = input.first().getSchema();
    //categoricalFeaturesInfo : Specifies which features are categorical and how many categorical values each of those
    //features can take. This is given as a map from feature index to the number of categories for that feature.
    //Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo =
      SparkUtils.getCategoricalFeatureInfo(schema, config.featuresToInclude, config.featuresToExclude,
                                           config.labelField, config.cardinalityMapping);
    final Map<String, Integer> fields = SparkUtils.getFeatureList(schema, config.featuresToInclude,
                                                                  config.featuresToExclude, config.labelField);

    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        List<Double> featureList = new ArrayList<>();
        List<Integer> featureIndex = new ArrayList<>();
        int counter = 0;
        for (String field : fields.keySet()) {
          if (record.get(field) != null) {
            featureList.add(((Number) record.get(field)).doubleValue());
            featureIndex.add(counter);
          }
          counter++;
        }
        Double prediction = record.get(config.labelField);
        if (prediction == null) {
          throw new IllegalArgumentException(String.format("Value of Label field %s value must not be null.",
                                                           config.labelField));
        }
        return new LabeledPoint(prediction, Vectors.sparse(counter, Ints.toArray(featureIndex),
                                                           Doubles.toArray(featureList)));
      }
    });

    trainingData.cache();
    final DecisionTreeModel model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, IMPURITY,
                                                                config.maxDepth, config.maxBins);
    // save the model to a file in the output FileSet
    JavaSparkContext sparkContext = context.getSparkContext();
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(JavaSparkContext.toSparkContext(sparkContext),
               outputFS.getBaseLocation().append(config.path).toURI().getPath());
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no-op; no need to do anything
  }

  /**
   * Configuration for DecisionTreeTrainer.
   */
  public static class DecisionTreeTrainerConfig extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Nullable
    @Description("A comma-separated sequence of fields to be used for training. If both featuresToInclude and \n" +
      "featuresToExclude are empty, all fields except the label will be used for training. Features to be used, must " +
      "be from one of the following types: int, long, float or double. Both featuresToInclude and featuresToExclude " +
      "fields cannot be specified.")
    private String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to be excluded when training. If both featuresToInclude and \n" +
      "featuresToExclude are empty, all fields except the label will be used for training. Both featuresToInclude " +
      "and featuresToExclude fields cannot be specified.")
    private String featuresToExclude;

    @Nullable
    @Description("Mapping of the feature to the cardinality of that feature; required for categorical features.")
    private String cardinalityMapping;

    @Description("The field from which to get the prediction. It must be of type double.")
    private String labelField;

    @Nullable
    @Description("Maximum depth of the tree.\n For example, depth 0 means 1 leaf node; depth 1 means 1 internal node " +
      "+ 2 leaf nodes. Default is 10.")
    private Integer maxDepth;

    @Nullable
    @Description("Maximum number of bins used for splitting when discretizing continuous featuresToInclude. " +
      "DecisionTree requires maxBins to be at least as large as the number of values in each categorical feature. " +
      "Default is 100.")
    private Integer maxBins;

    public DecisionTreeTrainerConfig() {
      maxDepth = 10;
      maxBins = 100;
    }

    public DecisionTreeTrainerConfig(String fileSetName, String path, @Nullable String featuresToInclude,
                                     @Nullable String featuresToExclude, @Nullable String cardinalityMapping,
                                     String labelField, @Nullable Integer maxDepth, @Nullable Integer maxBins) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.cardinalityMapping = cardinalityMapping;
      this.labelField = labelField;
      this.maxDepth = maxDepth;
      this.maxBins = maxBins;
    }

    public void validate(Schema inputSchema) {
      SparkUtils.validateConfigParameters(inputSchema, featuresToInclude, featuresToExclude, labelField,
                                          cardinalityMapping);
      SparkUtils.validateLabelFieldForTrainer(inputSchema, labelField);
    }
  }
}
