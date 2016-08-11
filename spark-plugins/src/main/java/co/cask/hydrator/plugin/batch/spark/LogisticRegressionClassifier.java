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
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.avro.reflect.Nullable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.Path;

/**
 * SparkCompute that uses a trained Logistic Regression model to classify and tag input records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(LogisticRegressionClassifier.PLUGIN_NAME)
@Description("Uses a trained Logistic Regression model to classify records.")
public class LogisticRegressionClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "LogisticRegressionClassifier";

  private Config config;
  private Schema outputSchema;

  /**
   * Configuration for the LogisticRegressionClassifier.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the FileSet to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Description("A space-separated sequence of words to classify.")
    private final String fieldsToClassify;

    @Description("The field on which to set the prediction. It will be of type double.")
    private final String predictionField;

    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer and equal to the" +
                  " number of features used in LogisticRegressionTrainer. The default value if none is provided " +
                  " will be 100.")
    private final Integer numFeatures;

    public Config(String fileSetName, String path, String fieldsToClassify, String predictionField,
                  Integer numFeatures) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.fieldsToClassify = fieldsToClassify;
      this.predictionField = predictionField;
      this.numFeatures = numFeatures;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      outputSchema = null;
      stageConfigurer.setOutputSchema(null);
      return;
    }
    validateSchema(inputSchema);

    // otherwise, we have a constant input schema. Get the input schema and
    // add a field to it, on which the prediction will be set
    outputSchema = getOutputSchema(inputSchema);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  private void validateSchema(Schema inputSchema) {
    Schema.Field predictionField = inputSchema.getField(config.predictionField);
    Preconditions.checkArgument(predictionField == null, "Prediction field must not already exist in input schema.");
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException("Failed to find model to use for classification." +
                                           " Location does not exist: " + modelLocation, null);
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);

    String modelPath = modelLocation.toURI().getPath();
    final LogisticRegressionModel loadedModel = LogisticRegressionModel.load(sparkContext, modelPath);

    final HashingTF tf = new HashingTF((config.numFeatures == null) ? 100 : config.numFeatures);
    final String[] columns = config.fieldsToClassify.split(",");
    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
        List<String> result = new ArrayList<>();
        for (String column : columns) {
          result.add(String.valueOf(structuredRecord.get(column)));
        }
        double prediction = loadedModel.predict(tf.transform(result));
        return cloneRecord(structuredRecord).set(config.predictionField, prediction).build();
      }
    });
    return output;
  }

  // creates a builder based off the given record
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schemaToUse = outputSchema != null ? outputSchema : getOutputSchema(record.getSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(schemaToUse);
    for (Schema.Field field : schemaToUse.getFields()) {
      if (!config.predictionField.equals(field.getName())) {
        builder.set(field.getName(), record.get(field.getName()));
      }
    }
    return builder;
  }

  private Schema getOutputSchema(Schema inputSchema) {
    return getOutputSchema(inputSchema, config.predictionField);
  }

  private Schema getOutputSchema(Schema inputSchema, String predictionField) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.DOUBLE)));
    return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request.predictionField);
  }

  /**
   * Endpoint request for output schema.
   */
  private static final class GetSchemaRequest {
    private Schema inputSchema;
    private String predictionField;
  }
}
