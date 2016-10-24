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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common class for Spark Predictors/Classifiers. Contains common Spark ML Classifiers properties, configuartion,
 * validation and feature extraction methods to be used in predictors/classifiers.
 */
public abstract class SparkMLPredictor extends SparkCompute<StructuredRecord, StructuredRecord> {

  private MLPredictorConfig config;
  private Schema outputSchema = null;

  /**
   * Config class for Predictors/Classifiers. Contains common config properties to be used in predictors/classifiers.
   */
  protected static class MLPredictorConfig extends PluginConfig {

    @Description("The name of the FileSet to load the model from.")
    protected String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    protected String path;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be used for classification/prediction.")
    protected String featureFieldsToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be excluded from being used " +
      "in classification/prediction.")
    protected String featureFieldsToExclude;

    @Description("The field on which to set the prediction. It will be of type double.")
    protected String predictionField;

    protected MLPredictorConfig(String fileSetName, String path, @Nullable String featureFieldsToInclude,
                                @Nullable String featureFieldsToExclude, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featureFieldsToInclude = featureFieldsToInclude;
      this.featureFieldsToExclude = featureFieldsToExclude;
      this.predictionField = predictionField;
    }

    public void validate(Schema inputSchema) {
      SparkUtils.validateConfigParameters(inputSchema, featureFieldsToInclude, featureFieldsToExclude, predictionField,
                                          null);
    }
  }

  public SparkMLPredictor(MLPredictorConfig config) {
    this.config = config;
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
      throw new IllegalArgumentException("Failed to find model to use for classification. " +
                                           "Location does not exist: " + modelLocation, null);
    }
    initialize(context.getSparkContext().sc(), modelLocation.toURI().getPath());
  }

  public abstract void initialize(SparkContext context, String modelPath);

  public abstract double predict(Vector vector);

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    if (input.isEmpty()) {
      return context.getSparkContext().emptyRDD();
    }
    Schema inputSchema = input.first().getSchema();
    outputSchema = (outputSchema != null) ? outputSchema :
      SparkUtils.getOutputSchema(inputSchema, config.predictionField);
    final Map<String, Integer> featuresList = SparkUtils.getFeatureList(inputSchema,
                                                                        config.featureFieldsToInclude,
                                                                        config.featureFieldsToExclude,
                                                                        config.predictionField);
    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord record) throws Exception {
        List<Double> featureList = new ArrayList<>();
        List<Integer> featureIndex = new ArrayList<>();
        int counter = 0;
        for (String field : featuresList.keySet()) {
          if (record.get(field) != null) {
            featureList.add(((Number) record.get(field)).doubleValue());
            featureIndex.add(counter);
          }
          counter++;
        }
        Vector vector = Vectors.sparse(counter, Ints.toArray(featureIndex), Doubles.toArray(featureList));
        double prediction = predict(vector);
        return SparkUtils.cloneRecord(record, outputSchema, config.predictionField).
          set(config.predictionField, prediction).build();
      }
    });
    return output;
  }
}
