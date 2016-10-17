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
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common class for Spark Trainers. Contains common Spark ML Trainer properties, configuartion ,validation and
 * feature extraction methods to be used in trainers.
 */
public abstract class SparkMLTrainer extends SparkSink<StructuredRecord> {

  private MLTrainerConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(SparkMLTrainer.class);

  /**
   * Config class for Trainers. Contains common config properties to be used in trainers.
   */
  protected class MLTrainerConfig extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    protected String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    protected String path;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be used for training.")
    protected String featureFieldsToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be excluded from being used in training.")
    protected String featureFieldsToExclude;

    @Description("The field from which to get the label. It must be of type double.")
    protected String labelField;

    protected MLTrainerConfig() {
    }

    protected MLTrainerConfig(String fileSetName, String path, @Nullable String featureFieldsToInclude,
                           @Nullable String featureFieldsToExclude, String labelField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featureFieldsToInclude = featureFieldsToInclude;
      this.featureFieldsToExclude = featureFieldsToExclude;
      this.labelField = labelField;
    }

    protected void validate(Schema inputSchema) {
      SparkUtils.validateConfigParameters(inputSchema, featureFieldsToInclude, featureFieldsToExclude, labelField,
                                          null);
      SparkUtils.validateLabelFieldForTrainer(inputSchema, labelField);
    }
  }

  public SparkMLTrainer(MLTrainerConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validate(inputSchema);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception { }

  protected Map<String, Integer> getFeatures(Schema inputSchema, @Nullable String featuresToInclude,
                                             @Nullable String featuresToExclude, String labelField) {
    return SparkUtils.getFeatureList(inputSchema, featuresToInclude, featuresToExclude, labelField);
  }

  public static JavaRDD<LabeledPoint> getTrainingData(final Map<String, Integer> fields,
                                                      JavaRDD<StructuredRecord> input, final String labelField) {
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
        Double prediction = record.get(labelField);
        if (prediction == null) {
          throw new IllegalArgumentException(String.format("Value of Label field %s value must not be null.",
                                                           labelField));
        }
        return new LabeledPoint(prediction, Vectors.sparse(counter, Ints.toArray(featureIndex),
                                                           Doubles.toArray(featureList)));
      }
    });
    return trainingData;
  }

  public SparkContext getSparkContext(SparkExecutionPluginContext context) {
    JavaSparkContext javaSparkContext = context.getSparkContext();
    return JavaSparkContext.toSparkContext(javaSparkContext);
  }

  public String getModelPath(SparkExecutionPluginContext context) throws IOException {
    FileSet outputFS = context.getDataset(config.fileSetName);
    return outputFS.getBaseLocation().append(config.path).toURI().getPath();
  }
}
