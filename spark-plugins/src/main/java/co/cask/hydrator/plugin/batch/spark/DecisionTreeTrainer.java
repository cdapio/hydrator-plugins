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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark sink plugin that trains the decision tree model for multiclass classification.
 * Write the model to the dataset.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(DecisionTreeTrainer.PLUGIN_NAME)
@Description("Creates a trained model for multi-class classification.")
public final class DecisionTreeTrainer extends SparkSink<StructuredRecord> {
  public static final Logger LOG = LoggerFactory.getLogger(DecisionTreeTrainer.class);
  public static final String PLUGIN_NAME = "DecisionTreeTrainer";

  private DecisionTreeConfig config;

  /**
   * Config for the DecisionTreeTrainer.
   */
  public static class DecisionTreeConfig extends PluginConfig {

    @Description("Name of the FileSet to save a model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Description("Number of classes.")
    private int numClasses;

    @Description("Classification field")
    private String classificationField;

    @Description("Data points")
    private String dataPoints;

    @Description("Maximum depth of decision tree")
    private Integer maxTreeDepth;

    @Nullable
    @Description("Maximum bin count")
    private Integer maxBinCount;

    @Nullable
    @Description("Impurity")
    private String impurity;
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

  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input)
    throws Exception {

    Preconditions.checkArgument(input.count() != 0, "Input RDD is empty.");

    double[] weight = {0.8, 0.1, 0.1};
    JavaRDD<StructuredRecord>[] javaRDDs = input.randomSplit(weight);

    final HashingTF tf = new HashingTF(100);
    JavaRDD<LabeledPoint> trainingData = javaRDDs[0].map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        String text = record.get(config.dataPoints);
        LOG.info("classificationField" + record.get(config.classificationField));
        return new LabeledPoint(Double.valueOf((String) record.get(config.classificationField)),
                                tf.transform(Lists.newArrayList(text.split(","))));
      }
    });

    trainingData.cache();

    JavaRDD<LabeledPoint> validationData = javaRDDs[1].map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        String text = record.get(config.dataPoints);
        LOG.info("classificationField" + record.get(config.classificationField));
        return new LabeledPoint(Double.valueOf((String) record.get(config.classificationField)),
                                tf.transform(Lists.newArrayList(text.split(","))));
      }
    });

    validationData.cache();

    RandomForestModel randomForestModel = null;
    double accuracy = -1;
    for (int treeDepth = 4; treeDepth < 5; treeDepth++) {
      Map<Integer, Integer> categories = new HashMap<>();
      categories.put(10, 4);
      categories.put(11, 40);
      final RandomForestModel model = RandomForest.trainClassifier(trainingData, config.numClasses,
                                                                   categories, 7, "auto", config.impurity,
                                                                   config.maxTreeDepth, 300, 3);

      JavaRDD<Tuple2<Object, Object>> predictionAndLabel = validationData.map(new Function<LabeledPoint,
        Tuple2<Object, Object>>() {
        @Override
        public Tuple2<Object, Object> call(LabeledPoint p) throws Exception {
          Double prediction = model.predict(p.features());
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      });

      MulticlassMetrics multiclassMetrics = new MulticlassMetrics(predictionAndLabel.rdd());
      LOG.info("SAGAR1 Accuracy with impurity {}, depth {} is {}.", config.impurity, treeDepth,
               multiclassMetrics.precision());

      if (multiclassMetrics.precision() > accuracy) {
        accuracy = multiclassMetrics.precision();
        randomForestModel = model;
      }
    }

    // save the model to a file in the output FileSet
    JavaSparkContext sparkContext = context.getSparkContext();
    FileSet outputFS = context.getDataset(config.fileSetName);

    if (randomForestModel != null) {
      randomForestModel.save(JavaSparkContext.toSparkContext(sparkContext),
                             outputFS.getBaseLocation().append(config.path).toURI().getPath());
    }
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no op
  }
}
