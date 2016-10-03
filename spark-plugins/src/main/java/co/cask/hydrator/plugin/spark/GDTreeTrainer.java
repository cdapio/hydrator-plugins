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
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that uses GD-Tree Classifier algorithm and trains a model based upon various labels
 * in the structured record. Writes this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(GDTreeTrainer.PLUGIN_NAME)
@Description("Uses GD-Tree to train a model based upon whether messages are spam or not.")
public class GDTreeTrainer extends SparkSink<StructuredRecord> {

  public static final String PLUGIN_NAME = "GDTreeTrainer";

  private Config config;

  public GDTreeTrainer(Config config) {
    this.config = config;
  }
  /**
   * Configuration for the GDTreeTrainer.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Nullable
    @Description("A comma-separated sequence of fields to use for training.")
    private String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to exclude from being used for training.")
    private String featuresToExclude;

    @Description("The field which is to be used as label. It must be of type double.")
    private String labelField;

    @Nullable
    @Description("Mapping of the feature to the cardinality of that feature; required for categorical features.")
    private String cardinalityMapping;

    @Nullable
    @Description("Maximum depth of the tree." +
      "For example, depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. Default is 10.")
    private Integer maxDepth;

    @Nullable
    @Description("The number of classes to use in training the model. It must be of type integer. " +
      "The default value if none is provided will be 2.")
    private Integer maxClass;

    @Nullable
    @Description("The number of trees in the model. Each iteration produces one tree. Increased iteration value " +
      "improves training data accuracy.")
    private Integer maxIteration;

    public Config() {
      this.maxDepth = 10;
      this.maxClass = 2;
      this.maxIteration = 10;
    }

    public Config(String fileSetName, String path, @Nullable String featuresToInclude,
                  @Nullable String featuresToExclude, String labelField, @Nullable String cardinalityMapping,
                  @Nullable Integer maxDepth, @Nullable Integer maxClass, @Nullable Integer maxIteration) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.cardinalityMapping = cardinalityMapping;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.labelField = labelField;
      this.maxDepth = maxDepth;
      this.maxClass = maxClass;
      this.maxIteration = maxIteration;
    }

    private void validate(Schema inputSchema) {
      SparkUtils.validateLabelFieldForTrainer(inputSchema, this.labelField);
      SparkUtils.validateConfigParameters(inputSchema, this.featuresToInclude, this.featuresToExclude, this.labelField,
                                          this.cardinalityMapping);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkExecutionPluginContext context, final JavaRDD<StructuredRecord> input) throws Exception {
    if (input == null) {
      throw new IllegalArgumentException("Input java rdd is null.");
    }
    Schema schema = input.first().getSchema();
    final Map<String, Integer> fields = SparkUtils.getFeatureList(schema, config.featuresToInclude,
                                                                  config.featuresToExclude, config.labelField);

    //categoricalFeaturesInfo : Specifies which features are categorical and how many categorical values each of those
    //features can take. This is given as a map from feature index to the number of categories for that feature.
    //Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo =
      SparkUtils.getCategoricalFeatureInfo(config.cardinalityMapping, fields);

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

    BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
    boostingStrategy.setNumIterations(config.maxIteration);
    boostingStrategy.getTreeStrategy().setNumClasses(config.maxClass);
    boostingStrategy.getTreeStrategy().setMaxDepth(config.maxDepth);

    boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

    final GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);

    // save the model to a file in the output FileSet
    JavaSparkContext sparkContext = context.getSparkContext();
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(JavaSparkContext.toSparkContext(sparkContext),
               outputFS.getBaseLocation().append(config.path).toURI().getPath());
  }
}
