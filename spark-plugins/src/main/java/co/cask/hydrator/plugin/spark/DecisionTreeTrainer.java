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
import com.google.common.base.Splitter;
import com.google.common.primitives.Doubles;
import joptsimple.internal.Strings;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon a label in the structured record using Decision Tree Regression.
 * Writes this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(DecisionTreeTrainer.PLUGIN_NAME)
@Description("Trains a regression model based upon a particular label and featuresToInclude of a record.")
public class DecisionTreeTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "DecisionTreeTrainer";
  //Impurity measure of the homogeneity of the labels at the node. Expected value for regression is "variance".
  private static final String IMPURITY = "variance";
  private static final int DEFAULT_MAX_DEPTH = 10;
  private static final int DEFAULT_MAX_BINS = 100;
  private DecisionTreeTrainerConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      config.validate(inputSchema);
    }
  }

  @Override
  public void run(SparkExecutionPluginContext context, final JavaRDD<StructuredRecord> input) throws Exception {

    Schema schema = input.first().getSchema();
    //categoricalFeaturesInfo : Specifies which features are categorical and how many categorical values each of those
    //features can take. This is given as a map from feature index to the number of categories for that feature.
    //Empty categoricalFeaturesInfo indicates all features are continuous.
    Map<Integer, Integer> categoricalFeaturesInfo = config.getCategoricalFeatureInfo(schema);
    final List<String> fields = config.getFeatureList(schema);

    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        List<Double> featureList = new ArrayList<>();
        for (String field : fields) {
          featureList.add(Double.valueOf(record.get(field).toString()));
        }
        Double prediction = record.get(config.labelField);
        if (prediction == null) {
          throw new IllegalArgumentException(String.format("Value of Prediction field %s value must not be null",
                                                           config.labelField));
        }
        return new LabeledPoint(prediction, Vectors.dense(Doubles.toArray(featureList)));
      }
    });

    trainingData.cache();

    config.maxDepth = config.maxDepth == null ? DEFAULT_MAX_DEPTH : config.maxDepth;
    config.maxBins = config.maxBins == null ? DEFAULT_MAX_BINS : config.maxBins;
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

    @Nullable
    @Description("A comma-separated sequence of fields to use for training. If empty, all the fields will be " +
      "considered for training. Features to be used, must be from one of the following type: int, long, float or " +
      "double. Both featuresToInclude and featuresToExclude fields cannot be specified.")
    private final String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to exclude when training. If empty, all the fields will be " +
      "considered for training. Both featuresToInclude and featuresToExclude fields cannot be specified.")
    private final String featuresToExclude;

    @Nullable
    @Description("Mapping of the feature to the cardinality of that feature; required for categorical features.")
    private final String cardinalityMapping;

    @Description("The field from which to get the prediction. It must be of type double.")
    private final String labelField;

    @Nullable
    @Description("Maximum depth of the tree.\n For example, depth 0 means 1 leaf node; depth 1 means 1 internal node " +
      "+ 2 leaf nodes. Default is 10.")
    private Integer maxDepth;

    @Nullable
    @Description("Maximum number of bins used for splitting when discretizing continuous featuresToInclude. " +
      "DecisionTree requires maxBins to be at least as large as the number of values in each categorical feature. " +
      "Default is 100.")
    private Integer maxBins;

    public DecisionTreeTrainerConfig(String fileSetName, String path, String featuresToInclude,
                                     String featuresToExclude, String cardinalityMapping, String labelField,
                                     Integer maxDepth, Integer maxBins) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.cardinalityMapping = cardinalityMapping;
      this.labelField = labelField;
      this.maxDepth = maxDepth;
      this.maxBins = maxBins;
    }

    private void validate(Schema inputSchema) {
      if (!Strings.isNullOrEmpty(featuresToExclude) && !Strings.isNullOrEmpty(featuresToInclude)) {
        throw new IllegalArgumentException("Cannot specify values for both featuresToInclude and featuresToExclude. " +
                                             "Please specify fields for one");
      }
      List<String> fields = getFeatureList(inputSchema);
      for (String field : fields) {
        Schema.Field inputField = inputSchema.getField(field);
        Schema schema = inputField.getSchema();
        Schema.Type features = schema.isNullableSimple() ? schema.getNonNullable().getType() : schema.getType();
        Preconditions.checkArgument(features.isSimpleType(), "Field to be used for training must be of type number: " +
          "int, long, float, double but was of type %s for field %s ", features, field);
        if (features.equals(Schema.Type.BYTES) || features.equals(Schema.Type.STRING) ||
          features.equals(Schema.Type.BOOLEAN)) {
          throw new IllegalArgumentException(String.format("Field to be used for training must be of type : int," +
                                                             " double, float, long but was of type %s for field %s ",
                                                           features, field));
        }
        Preconditions.checkArgument(!features.equals(Schema.Type.NULL), "Field to classify must not be of type null");
      }
      Schema.Field prediction = inputSchema.getField(labelField);
      if (labelField == null) {
        throw new IllegalArgumentException(String.format("Prediction field %s does not exists in the input schema",
                                                         labelField));
      }
      Schema predictionSchema = prediction.getSchema();
      Schema.Type predictionFieldType = predictionSchema.isNullableSimple() ?
        predictionSchema.getNonNullable().getType() : predictionSchema.getType();
      if (predictionFieldType != Schema.Type.DOUBLE) {
        throw new IllegalArgumentException(String.format("Prediction field must be of type Double, but was %s.",
                                                         predictionFieldType));
      }
    }

    private List<String> getFeatureList(Schema inputSchema) {
      List<String> fields = new LinkedList<>();
      List<Schema.Field> inputSchemaFields = inputSchema.getFields();
      List<String> excludeFeatures = new ArrayList<>();

      if (!Strings.isNullOrEmpty(featuresToInclude)) {
        fields = Arrays.asList(featuresToInclude.split(","));
        for (String field : fields) {
          field = field.trim();
          Schema.Field inputField = inputSchema.getField(field);
          if (inputField == null) {
            throw new IllegalArgumentException(String.format("Field %s does not exists in the input schema", field));
          }
        }
      } else {
        if (!Strings.isNullOrEmpty(featuresToExclude)) {
          excludeFeatures = Arrays.asList(featuresToExclude.split(","));
        }
        for (Schema.Field field : inputSchemaFields) {
          String fieldName = field.getName();
          if (!fieldName.equals(labelField)) {
            if (!excludeFeatures.isEmpty()) {
              if (!excludeFeatures.contains(fieldName)) {
                fields.add(fieldName);
              }
            } else {
              fields.add(fieldName);
            }
          }
        }
      }
      return fields;
    }

    private Map<Integer, Integer> getCategoricalFeatureInfo(Schema inputSchema) {
      List<String> featureList = getFeatureList(inputSchema);
      Map<Integer, Integer> outputFieldMappings = new HashMap<>();
      if (!Strings.isNullOrEmpty(cardinalityMapping)) {
        for (String field : Splitter.on(',').trimResults().split(cardinalityMapping)) {
          String[] value = field.split(":");
          if (value.length == 2) {
            outputFieldMappings.put(featureList.indexOf(value[0]), Integer.parseInt(value[1]));
          } else {
            throw new IllegalArgumentException(
              String.format("Either key or value is missing for 'Categorical Feature  Cardinality Mapping'. Please " +
                              "make sure both key and value are present."));
          }
        }
      }
      return outputFieldMappings;
    }
  }
}


