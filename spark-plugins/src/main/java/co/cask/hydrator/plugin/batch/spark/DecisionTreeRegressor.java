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
import com.google.common.primitives.Doubles;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Path;

/**
 * SparkCompute that uses a trained model to tag input records using Decision Tree regression.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(DecisionTreeRegressor.PLUGIN_NAME)
@Description("Uses a trained Decision Tree Regression modal and regress records.")
public class DecisionTreeRegressor extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "DecisionTreeRegressor";
  private DecisionTreeRegressorConfig config;
  private Schema outputSchema;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or has multiple schemas.
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
    String[] fields = config.features.split(",");
    for (String field : fields) {
      Schema.Field inputField = inputSchema.getField(field);
      if (inputField == null) {
        throw new IllegalArgumentException(String.format("Field %s does not exists in the input schema", field));
      }
      Schema.Type features = inputField.getSchema().getType();
      Preconditions.checkArgument(features.isSimpleType(), "Field to classify must be of simple type : String, int, " +
        "double, float, long, bytes, boolean but was %s.", features);
      Preconditions.checkArgument(!features.equals(Schema.Type.NULL), "Field to classify must not be of type null");
    }
    Schema.Field predictionField = inputSchema.getField(config.predictionField);
    Preconditions.checkArgument(predictionField == null, "Prediction field must not already exist in the input " +
      "schema.");
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      throw new IllegalArgumentException(String.format("Failed to find model to use for Regression. Location does " +
                                                         "not exist: {}.", modelLocation));
    }

    final HashMap<String, Map<Object, Long>> categoricalFeaturesMap = new HashMap<>();
    final String[] fields = config.features.split(",");

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

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    final DecisionTreeModel loadedModel = DecisionTreeModel.load(sparkContext, modelLocation.toURI().getPath());

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord record) throws Exception {
        List<Double> featureList = new ArrayList<Double>();
        for (String field : fields) {
          if (categoricalFeaturesMap.keySet().contains(field)) {
            featureList.add((categoricalFeaturesMap.get(field).get(record.get(field))).doubleValue());
          } else {
            featureList.add(new Double(record.get(field).toString()));
          }
        }

        Vector vector = Vectors.dense(Doubles.toArray(featureList));
        double prediction = loadedModel.predict(vector);

        return cloneRecord(record)
          .set(config.predictionField, prediction)
          .build();
      }
    });
    return output;
  }

  /**
  *Creates a builder based off the given record.
  */
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schemaToUse = outputSchema != null ? outputSchema : getOutputSchema(record.getSchema());
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(schemaToUse.getFields());
    fields.addAll(Arrays.asList(Schema.Field.of(config.predictionField, Schema.of(Schema.Type.DOUBLE))));
    schemaToUse = Schema.recordOf("records", fields);
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
   * Configuration for the DecisionTreeRegressor.
   */
  public static class DecisionTreeRegressorConfig extends PluginConfig {

    @Description("The name of the FileSet to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Description("A comma-separated sequence of fields for regression. Features to be used, must be of simple type: " +
      "String, int, double, float, long, bytes, boolean.")
    private final String features;

    @Description("The field on which to set the prediction. It will be of type double.")
    private final String predictionField;

    public DecisionTreeRegressorConfig(String fileSetName, String path, String features, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.features = features;
      this.predictionField = predictionField;
    }
  }

  /**
   * Endpoint request for output schema.
   */
  private static final class GetSchemaRequest {
    private Schema inputSchema;
    private String predictionField;
  }
}
