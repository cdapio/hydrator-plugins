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
package co.cask.hydrator.plugin.batch.spark;

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
import org.apache.avro.reflect.Nullable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    if (inputSchema != null) {
      validateSchema(inputSchema);
    }
  }

  private void validateSchema(Schema inputSchema) {
    String[] fields = config.features.split(",");
    for (String field : fields) {
      Schema.Field inputField = inputSchema.getField(field);
      if (inputField == null) {
        throw new IllegalArgumentException(String.format("Field %s does not exists in the input schema", field));
      }
      Schema.Type features = inputField.getSchema().getType();
      Preconditions.checkArgument(features.isSimpleType(), "Field to classify must be of simple type : String, int, " +
        "double, float, long, bytes, boolean but was %s.", features);
      Preconditions.checkArgument(!features.equals(Schema.Type.NULL), "Field to classify must not be of " +
        "type null");
    }
    Schema.Type predictionFieldType = inputSchema.getField(config.predictionField).getSchema().getType();
    Preconditions.checkArgument(predictionFieldType == Schema.Type.DOUBLE,
                                "Prediction field must be of type Double, but was %s.", predictionFieldType);
  }

  @Override
  public void run(SparkExecutionPluginContext context, final JavaRDD<StructuredRecord> input) throws Exception {
    //categoricalFeaturesInfo : Specifies which features are categorical and how many categorical values each of those
    //features can take. This is given as a map from feature index to the number of categories for that feature.
    //Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    final HashMap<String, Map<Object, Long>> categoricalFeaturesMap = new HashMap<>();
    final String[] fields = config.features.split(",");
    List<String> categoricalFeaturesList = Arrays.asList(fields);

    for (final String field : fields) {
      Map<Object, Long> map = input.map(new Function<StructuredRecord, Object>() {
        @Override
        public Object call(StructuredRecord structuredRecord) throws Exception {
          Schema schema = structuredRecord.getSchema().getField(field).getSchema();
          Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
          if (!(type.equals(Schema.Type.INT) || type.equals(Schema.Type.DOUBLE) || type.equals(Schema.Type.FLOAT) ||
            type.equals(Schema.Type.LONG))) {
            return structuredRecord.get(field);
          } else {
            return null;
          }
        }
      }).distinct().zipWithIndex().collectAsMap();

      if (!(map == null ||
        (map.size() == 1 && map.keySet().contains(null)))) {
        categoricalFeaturesMap.put(field, map);
      }
    }

    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        List<Double> doubles = new ArrayList<Double>();
        for (String field : fields) {
          if (categoricalFeaturesMap.keySet().contains(field)) {
            doubles.add((categoricalFeaturesMap.get(field).get(record.get(field))).doubleValue());
          } else {
            doubles.add(new Double(record.get(field).toString()));
          }
        }
        return new LabeledPoint((Double) record.get(config.predictionField), Vectors.dense(Doubles.toArray(doubles)));
      }
    });

    trainingData.cache();

    for (String key : categoricalFeaturesMap.keySet()) {
      categoricalFeaturesInfo.put(categoricalFeaturesList.indexOf(key), categoricalFeaturesMap.get(key).size());
    }

    config.maxDepth = config.maxDepth == null ? 10 : config.maxDepth;
    config.maxBins = config.maxBins == null ? 100 : config.maxBins;
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
    private final String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private final String path;

    @Description("A comma-separated sequence of fields to use for training. Features to be used, must be of simple " +
      "type: String, int, double, float, long, bytes, boolean.")
    private final String features;

    @Description("The field from which to get the prediction. It must be of type double.")
    private final String predictionField;

    @Nullable
    @Description("Maximum depth of the tree.\n For example, depth 0 means 1 leaf node; depth 1 means 1 internal node " +
      "+ 2 leaf nodes. Default is 10.")
    private Integer maxDepth;

    @Nullable
    @Description("Maximum number of bins used for splitting when discretizing continuous features. DecisionTree " +
      "requires maxBins to be at least as large as the number of values in each categorical feature. Default is 100.")
    private Integer maxBins;

    public DecisionTreeTrainerConfig(String fileSetName, String path, String features, String predictionField,
                                     Integer maxDepth, Integer maxBins) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.features = features;
      this.predictionField = predictionField;
      this.maxDepth = maxDepth;
      this.maxBins = maxBins;
    }
  }
}
