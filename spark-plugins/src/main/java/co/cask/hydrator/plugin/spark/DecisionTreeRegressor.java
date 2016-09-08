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
import joptsimple.internal.Strings;
import org.apache.avro.reflect.Nullable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.twill.filesystem.Location;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.Path;

/**
 * SparkCompute that uses a trained model to tag input records using Decision Tree regression.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(DecisionTreeRegressor.PLUGIN_NAME)
@Description("Uses a trained Decision Tree Regression model and regress records.")
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
    config.validate(inputSchema);
    // otherwise, we have a constant input schema. Get the input schema and
    // add a field to it, on which the prediction will be set
    outputSchema = getOutputSchema(inputSchema);
    stageConfigurer.setOutputSchema(outputSchema);
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
    final List<String> fields = config.getFeatureList(input.first().getSchema());

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    final DecisionTreeModel loadedModel = DecisionTreeModel.load(sparkContext, modelLocation.toURI().getPath());

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord record) throws Exception {
        List<Double> featureList = new ArrayList<>();
        for (String field : fields) {
          featureList.add(Double.valueOf(record.get(field).toString()));
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
   * Creates a builder based off the given record.
   */
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schemaToUse = (outputSchema != null) ? outputSchema : getOutputSchema(record.getSchema());
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

    @Nullable
    @Description("A comma-separated sequence of fields to be used for Decision Tree Regression. Features to be used, " +
      "must be from one of the following type: int, long, float or double. Both featuresToInclude and " +
      "featuresToExclude fields cannot be specified.")
    private final String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to be excluded when calculating prediction. Both " +
      "featuresToInclude and featuresToExclude fields cannot be specified.")
    private final String featuresToExclude;

    @Description("The field on which to set the prediction. It will be of type double.")
    private final String predictionField;

    public DecisionTreeRegressorConfig(String fileSetName, String path, String featuresToInclude,
                                       String featuresToExclude, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.predictionField = predictionField;
    }

    private void validate(Schema inputSchema) {
      if (!Strings.isNullOrEmpty(featuresToExclude) && !Strings.isNullOrEmpty(featuresToInclude)) {
        throw new IllegalArgumentException("Cannot specify values for both featuresToInclude and featuresToExclude. " +
                                             "Please specify fields for one");
      }
      List<String> fields = getFeatureList(inputSchema);
      for (String field : fields) {
        Schema schema = inputSchema.getField(field).getSchema();
        Schema.Type features = schema.isNullableSimple() ? schema.getNonNullable().getType() : schema.getType();
        Preconditions.checkArgument(features.isSimpleType(), "Field to be used must be of type number: int, long, " +
          "float, double but was of type %s for field %s ", features, field);
        if (features.equals(Schema.Type.BYTES) || features.equals(Schema.Type.STRING) ||
          features.equals(Schema.Type.BOOLEAN)) {
          throw new IllegalArgumentException(String.format("Field to classify must be of type : int, double, " +
                                                             "float, long but was of type %s for field %s ", features,
                                                           field));
        }
        Preconditions.checkArgument(!features.equals(Schema.Type.NULL), "Field to classify must not be of type null");
      }
      Preconditions.checkArgument(inputSchema.getField(predictionField) == null, "Prediction field must not already " +
        "exist in the input schema.");
    }

    private List<String> getFeatureList(Schema inputSchema) {
      List<String> fields = new ArrayList<>();
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
          if (!fieldName.equals(predictionField)) {
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
  }

  /**
   * Endpoint request for output schema.
   */
  private static final class GetSchemaRequest {
    private Schema inputSchema;
    private String predictionField;
  }
}
