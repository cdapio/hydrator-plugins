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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.dmg.pmml.Array;
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * SparkCompute that uses a trained model to classify records
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(KMeansClassifier.PLUGIN_NAME)
@Description("Uses a trained KMeans model to classify records.")
public class KMeansClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "KMeansClassifier";

  private Config config;
  private Schema outputSchema;

  /**
   * Configuration for the KMeansClassifier.
   */
  public static class Config extends PluginConfig {

    @Description("Path of the file to load the model from.")
    private final String path;

    @Description("A space-separated sequence of words to classify.")
    private final String fieldToClassify;

    @Description("The field on which to set the prediction. It will be of type double.")
    private final String predictionField;

    public Config(String path, String fieldToClassify, String predictionField) {
      this.path = path;
      this.fieldToClassify = fieldToClassify;
      this.predictionField = predictionField;
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
    Schema.Type fieldToClassifyType = inputSchema.getField(config.fieldToClassify).getSchema().getType();
    Preconditions.checkArgument(fieldToClassifyType == Schema.Type.STRING,
                                "Field to classify must be of type String, but was %s.", fieldToClassifyType);
    Schema.Type predictionFieldType = inputSchema.getField(config.predictionField).getSchema().getType();
    Preconditions.checkArgument(predictionFieldType == Schema.Type.DOUBLE,
                                "Prediction field must be of type Double, but was %s.", predictionFieldType);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    String fileName = config.path;
    File file = new File(fileName);
    InputStream in = new FileInputStream(file);
    PMML pmml = JAXBUtil.unmarshalPMML(ImportFilter.apply((new InputSource(in))));

    final KMeansModel loadedModel = pmmlToKMeansModel(pmml);

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(config.fieldToClassify);
        String[] sarray = text.split(" ");
        double[] values = new double[sarray.length];
        for (int i = 0; i < sarray.length; i++) {
          values[i] = Double.parseDouble(sarray[i]);
        }
        Vector vector = Vectors.dense(values);
        double prediction = loadedModel.predict(vector);

        return cloneRecord(structuredRecord)
          .set(config.predictionField, prediction)
          .build();
      }
    });
    return output;
  }

  private static KMeansModel pmmlToKMeansModel(PMML pmml) {
    ClusteringModel clusteringModel = (ClusteringModel) pmml.getModels().get(0);
    List<Cluster> clusters = clusteringModel.getClusters();
    Vector[] clusterCenters = new Vector[clusters.size()];
    int i = 0;
    for (Cluster cluster : clusters) {
      clusterCenters[i++] = toVectorFromPMMLArray(cluster.getArray());
    }
    return new KMeansModel(clusterCenters);
  }

  private static Vector toVectorFromPMMLArray(Array array) {
    String[] values = array.getValue().split(" ");
    double[] doubles = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      doubles[i] = Double.valueOf(values[i]);
    }
    return Vectors.dense(doubles);
  }

  // creates a builder based off the given record
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schemaToUse = outputSchema != null ? outputSchema : getOutputSchema(record.getSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(schemaToUse);
    for (Schema.Field field : schemaToUse.getFields()) {
      if (config.predictionField.equals(field.getName())) {
        // don't copy the field to set from the input record; it will be set later
        continue;
      }
      builder.set(field.getName(), record.get(field.getName()));
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
}
