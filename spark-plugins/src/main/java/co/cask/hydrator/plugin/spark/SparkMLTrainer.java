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
import co.cask.hydrator.common.spark.SparkUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.twill.filesystem.Location;

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

  /**
   * Config class for Trainers. Contains common config properties to be used in trainers.
   */
  protected static class MLTrainerConfig extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    protected String fileSetName;

    @Nullable
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

    protected MLTrainerConfig(String fileSetName, @Nullable String path, @Nullable String featureFieldsToInclude,
                              @Nullable String featureFieldsToExclude, String labelField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featureFieldsToInclude = featureFieldsToInclude;
      this.featureFieldsToExclude = featureFieldsToExclude;
      this.labelField = labelField;
    }

    public void validate(Schema inputSchema) {
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

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    if (!input.isEmpty()) {
      final Schema inputSchema = input.first().getSchema();
      final Map<String, Integer> featuresList = SparkUtils.getFeatureList(inputSchema, config.featureFieldsToInclude,
                                                                          config.featureFieldsToExclude,
                                                                          config.labelField);

      // Filter out the records for which label is invalid
      JavaRDD<StructuredRecord> filteredData = input.filter(new Function<StructuredRecord, Boolean>() {
        @Override
        public Boolean call(StructuredRecord record) throws Exception {
          if (record.get(config.labelField) == null) {
            return false;
          }
          return true;
        }
      });

      JavaRDD<LabeledPoint> trainingData = filteredData.map(new Function<StructuredRecord, LabeledPoint>() {
        @Override
        public LabeledPoint call(StructuredRecord record) throws Exception {
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

          return new LabeledPoint((Double) record.get(config.labelField),
                                  Vectors.sparse(counter, Ints.toArray(featureIndex), Doubles.toArray(featureList)));
        }
      });
      trainingData.cache();
      FileSet outputFS = context.getDataset(config.fileSetName);
      Location modelLocation = outputFS.getBaseLocation();
      if (!Strings.isNullOrEmpty(config.path)) {
        modelLocation = modelLocation.append(config.path);
      }
      trainModel(context.getSparkContext().sc(), inputSchema, trainingData, modelLocation.toURI().getPath());
    }
  }

  public abstract void trainModel(SparkContext context, Schema inputSchema, JavaRDD<LabeledPoint> trainingData,
                                  String outputPath);
}
