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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Spark Sink plugin that trains a model and writes this model to a pmml file.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(KMeansTrainer.PLUGIN_NAME)
@Description("Trains a model for KMeansTrainer")
public class KMeansTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "KMeansTrainer";

  private Config config;

  /**
   * Configuration for the KMeansTrainer.
   */
  public static class Config extends PluginConfig {

    @Description("Path of the file to save the model to.")
    private final String path;

    @Description("A space-separated sequence of words to use for training.")
    private final String fieldToClassify;

    @Description("The field from which to get the prediction. It must be of type double.")
    private final String predictionField;

    public Config(String path, String fieldToClassify, String predictionField) {
      this.path = path;
      this.fieldToClassify = fieldToClassify;
      this.predictionField = predictionField;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      validateSchema(inputSchema);
    }
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
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    Preconditions.checkArgument(input.count() != 0, "Input RDD is empty.");

    final HashingTF tf = new HashingTF(100);
    JavaRDD<Vector> parsedData = input.map(new Function<StructuredRecord, Vector>() {
      @Override
      public Vector call(StructuredRecord record) throws Exception {
        String text = record.get(config.fieldToClassify);
        String[] sarray = text.split(" ");
        double[] values = new double[sarray.length];
        for (int i = 0; i < sarray.length; i++) {
          values[i] = Double.parseDouble(sarray[i]);
        }
        return Vectors.dense(values);
      }
    });

    parsedData.cache();

    // Cluster the data into two classes using KMeans
    int numClusters = 2;
    int numIterations = 20;
    final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

    String pmml = clusters.toPMML();
    System.out.println(pmml);
    String fileName = config.path;
    writeToFile(pmml, fileName);
  }


  private void writeToFile(String pmml, String fileName) throws IOException {
    File file = new File(fileName);
    FileOutputStream fop = new FileOutputStream(file);
    if (!file.exists()) {
      file.createNewFile();
    }
    byte[] contentInBytes = pmml.getBytes();
    fop.write(contentInBytes);
    fop.flush();
    fop.close();
  }
}
