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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark Sink plugin that trains a model based upon a label in the structured record using Linear Regression.
 * Writes this model to a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(LinearRegressionTrainer.PLUGIN_NAME)
@Description("Trains a regression model based upon a particular label and features of a record.")
public class LinearRegressionTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "LinearRegressionTrainer";
  private LinearRegressionTrainerConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void run(SparkExecutionPluginContext context, final JavaRDD<StructuredRecord> input) throws Exception {

    if (input == null) {
      throw new IllegalArgumentException("Input java rdd is null.");
    }
    Schema schema = input.first().getSchema();
    final Map<String, Integer> fields = SparkUtils.getFeatureList(schema, config.featuresToInclude,
                                                                  config.featuresToExclude, config.labelField);

    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
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
        Double prediction = record.get(config.labelField);
        if (prediction == null) {
          throw new IllegalArgumentException(String.format("Value of Label field %s value must not be null.",
                                                           config.labelField));
        }
        return new LabeledPoint(prediction, Vectors.sparse(counter, Ints.toArray(featureIndex),
                                                           Doubles.toArray(featureList)));
      }
    });

    trainingData.cache();
    final LinearRegressionModel model = LinearRegressionWithSGD.train(trainingData.rdd(), config.numIterations,
                                                                      config.stepSize);
    // save the model to a file in the output FileSet
    JavaSparkContext sparkContext = context.getSparkContext();
    FileSet outputFS = context.getDataset(config.fileSetName);
    model.save(JavaSparkContext.toSparkContext(sparkContext),
               outputFS.getBaseLocation().append(config.path).toURI().getPath());

  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no-op; no need to do anything
  }

  /**
   * Configuration for LinearRegressionTrainer.
   */
  public static class LinearRegressionTrainerConfig extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Nullable
    @Description("A comma-separated sequence of fields to use for training. If empty, all fields except the label " +
      "will be used for training. Features to be used, must be from one of the following types: int, long, float or " +
      "double. Both featuresToInclude and featuresToExclude fields cannot be specified.")
    private String featuresToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields to excluded when training. If empty, all fields except the " +
      "label will be used for training.  Both featuresToInclude and featuresToExclude fields cannot be specified.")
    private String featuresToExclude;

    @Description("The field from which to get the prediction. It must be of type double.")
    private String labelField;

    @Nullable
    @Description("The number of iterations to be used for training the model. It must be of type Integer. Default is " +
      "100.")
    private Integer numIterations;

    @Nullable
    @Description("The step size to be used for training the model. It must be of type Double. Default is 1.0.")
    private Double stepSize;

    public LinearRegressionTrainerConfig() {
      numIterations = 100;
      stepSize = 1.0;
    }

    public LinearRegressionTrainerConfig(String fileSetName, String path, @Nullable String featuresToInclude,
                                         @Nullable String featuresToExclude, String labelField,
                                         @Nullable Integer numIterations, @Nullable Double stepSize) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featuresToInclude = featuresToInclude;
      this.featuresToExclude = featuresToExclude;
      this.labelField = labelField;
      this.numIterations = numIterations;
      this.stepSize = stepSize;
    }

    public void validate(Schema inputSchema) {
      SparkUtils.validateConfigParameters(inputSchema, featuresToInclude, featuresToExclude, labelField, null);
      SparkUtils.validateLabelFieldForTrainer(inputSchema, labelField);
    }
  }
}
