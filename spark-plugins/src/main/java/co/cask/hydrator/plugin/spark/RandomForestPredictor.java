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
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * SparkCompute that uses a trained model to tag input records using Random Forest Regression.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(RandomForestPredictor.PLUGIN_NAME)
@Description("Uses a Random Forest Regression model to make predictions.")
public class RandomForestPredictor extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "RandomForestPredictor";
  private RandomForestPredictorConfig config;
  private Schema outputSchema;
  private RandomForestModel loadedModel;

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
    super.initialize(context);
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException(String.format("Failed to find model to use for Regression. Location does " +
                                                         "not exist: {}.", modelLocation));
    }
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    // load the model from a file in the model fileset
    loadedModel = RandomForestModel.load(sparkContext, modelLocation.toURI().getPath());
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {
    if (input == null) {
      throw new IllegalArgumentException("Input JavaRDD is null.");
    }
    Schema schema = input.first().getSchema();
    outputSchema = (outputSchema != null) ? outputSchema : SparkUtils.getOutputSchema(schema, config.predictionField);
    final Map<String, Integer> fields = SparkUtils.getFeatureList(schema, config.featuresToInclude,
                                                                  config.featuresToExclude, config.predictionField);

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
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

        return SparkUtils.cloneRecord(record, outputSchema, config.predictionField)
          .set(config.predictionField, prediction)
          .build();
      }
    });
    return output;
  }

  /**
   * Configuration for the RandomForestPredictor.
   */
  public static class RandomForestPredictorConfig extends PluginConfig {

    @Description("The name of the FileSet to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Nullable
    @Description("A comma-separated sequence of fields to be used for prediction.  If both featuresToInclude and " +
      "featuresToExclude are empty, all fields will be used for prediction. Features to be used, must be from one of " +
      "the following types: int, long, float or double. Both featuresToInclude and featuresToExclude fields cannot " +
      "be specified.")
    private final String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to be excluded for prediction.  If both featuresToInclude and " +
      "featuresToExclude are empty, all fields will be used for prediction. Both featuresToInclude and " +
      "featuresToExclude fields cannot be specified.")
    private final String featuresToExclude;

    @Description("The field on which to set the prediction. It will be of type double.")
    private final String predictionField;

    public RandomForestPredictorConfig(String fileSetName, String path, @Nullable String featuresToInclude,
                                       @Nullable String featuresToExclude, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.predictionField = predictionField;
    }

    public void validate(Schema inputSchema) {
      SparkUtils.validateConfigParameters(inputSchema, featuresToInclude, featuresToExclude, predictionField, null);
      Preconditions.checkArgument(inputSchema.getField(predictionField) == null, "Prediction field must not already " +
        "exist in the input schema.");
    }

  }
}
