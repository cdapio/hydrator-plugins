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
import com.google.common.collect.Lists;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Multiclass classifier.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(DecisionTreeClassifier.PLUGIN_NAME)
@Description("Uses decision tree classifier to classify records")
public class DecisionTreeClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DecisionTreeClassifier.class);
  public static final String PLUGIN_NAME = "DecisionTreeClassifier";
  private DecisionTreeConfig config;
  private Schema outputSchema;

  /**
   *
   */
  public static class DecisionTreeConfig extends PluginConfig {
    @Description("The name of the FileSet to load the model from.")
    private String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private String path;

    @Description("data points.")
    private String dataPoints;

    @Description("The field on which to classify.")
    private String classificationField;
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

  }

  private Schema getOutputSchema(Schema inputSchema) {
    return getOutputSchema(inputSchema, config.classificationField);
  }

  private Schema getOutputSchema(Schema inputSchema, String predictionField) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(predictionField, Schema.of(Schema.Type.DOUBLE)));
    return Schema.recordOf(inputSchema.getRecordName() + ".predicted", fields);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      LOG.warn("Failed to find model to use for classification. Location does not exist: {}.", modelLocation);
      return input;
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    final DecisionTreeModel loadedModel = DecisionTreeModel.load(sparkContext, modelLocation.toURI().getPath());

    final HashingTF tf = new HashingTF(100);

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(config.dataPoints);
        Vector vector = tf.transform(Lists.newArrayList(text.split(",")));
        double prediction = loadedModel.predict(vector);

        return cloneRecord(structuredRecord)
          .set(config.classificationField, prediction)
          .build();
      }
    });
    return output;
  }

  // creates a builder based off the given record
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schemaToUse = outputSchema != null ? outputSchema : getOutputSchema(record.getSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(schemaToUse);
    for (Schema.Field field : schemaToUse.getFields()) {
      if (config.classificationField.equals(field.getName())) {
        // don't copy the field to set from the input record; it will be set later
        continue;
      }
      builder.set(field.getName(), record.get(field.getName()));
    }
    return builder;
  }
}
