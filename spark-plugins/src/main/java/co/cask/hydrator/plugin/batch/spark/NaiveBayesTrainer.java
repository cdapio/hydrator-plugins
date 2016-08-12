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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.reflect.Nullable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Spark Sink plugin that trains a model based upon a label in the structured record.
 * Writes this model to a file of a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(NaiveBayesTrainer.PLUGIN_NAME)
@Description("Trains a model based upon whether messages are spam or not.")
public final class NaiveBayesTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "NaiveBayesTrainer";

  private Config config;

  /**
   * Configuration for the NaiveBayesTrainer.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    private final String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private final String path;

    @Description("A space-separated sequence of words to use for training.")
    private final String fieldToClassify;

    @Description("The field from which to get the prediction. It must be of type double.")
    private final String predictionField;

    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer and equal to the" +
                  " number of features used in NaiveBayesClassifier. The default value if none is provided will be" +
                  " 100.")
    private final Integer numFeatures;

    public Config(String fileSetName, String path, String fieldToClassify, String predictionField,
                  Integer numFeatures) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.fieldToClassify = fieldToClassify;
      this.predictionField = predictionField;
      this.numFeatures = numFeatures;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);

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

    final HashingTF tf = new HashingTF((config.numFeatures == null) ? 100 : config.numFeatures);
    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        String text = record.get(config.fieldToClassify);
        return new LabeledPoint((Double) record.get(config.predictionField),
                                tf.transform(Lists.newArrayList(text.split(" "))));
      }
    });

    trainingData.cache();

    final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);

    // save the model to a file in the output FileSet
    JavaSparkContext sparkContext = context.getSparkContext();
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(JavaSparkContext.toSparkContext(sparkContext),
               outputFS.getBaseLocation().append(config.path).toURI().getPath());
  }
}
