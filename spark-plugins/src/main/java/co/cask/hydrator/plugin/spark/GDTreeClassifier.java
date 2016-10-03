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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * SparkCompute that uses a GD-Tree Classifier model to classify and tag input records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(GDTreeClassifier.PLUGIN_NAME)
@Description("Uses a trained GDTree model to classify records.")
public class GDTreeClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "GDTreeClassifier";
  private final Config config;
  private Schema outputSchema;
  private GradientBoostedTreesModel loadedModel = null;

  public GDTreeClassifier(Config config) {
    this.config = config;
  }
  /**
   * Configuration for the GDTreeClassifier.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the FileSet to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from .")
    private final String path;

    @Nullable
    @Description("A comma-separated sequence of fields to use for classification.")
    private String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to exclude from being used for training.")
    private String featuresToExclude;

    @Description("The field on which prediction needs to be set. It must be of type double")
    private final String predictionField;

    public Config(String fileSetName, String path, @Nullable String featuresToInclude,
                  @Nullable String featuresToExclude, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.predictionField = predictionField;
    }

    private void validate(Schema inputSchema) {
      Schema.Field predictionField = inputSchema.getField(this.predictionField);
      Preconditions.checkArgument(predictionField == null, "Prediction field must not already exist in input schema.");
      SparkUtils.validateConfigParameters(inputSchema, this.featuresToInclude, this.featuresToExclude,
                                          this.predictionField, null);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
    stageConfigurer.setOutputSchema(SparkUtils.getOutputSchema(inputSchema, config.predictionField));
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException(String.format("Failed to find model to use for Regression. Location does " +
                                                         "not exist: {}.", modelLocation));
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    loadedModel = GradientBoostedTreesModel.load(sparkContext, modelLocation.toURI().getPath());
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (input == null) {
      throw new IllegalArgumentException("Input java rdd is null.");
    }
    final Schema inputSchema = input.first().getSchema();
    final Map<String, Integer> fields = SparkUtils.getFeatureList(inputSchema, config.featuresToInclude,
                                                                  config.featuresToExclude, config.predictionField);

    final JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord record) throws Exception {
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

        Vector vector = Vectors.sparse(counter, Ints.toArray(featureIndex), Doubles.toArray(featureList));
        double prediction = loadedModel.predict(vector);

        outputSchema = (outputSchema != null) ? outputSchema :
          SparkUtils.getOutputSchema(inputSchema, config.predictionField);

        return SparkUtils.cloneRecord(record, outputSchema, config.predictionField)
          .set(config.predictionField, prediction)
          .build();
      }
    });
    return output;
  }
}
