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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Spark Sink plugin that trains a model based upon whether messages are spam or not, and then classifies messages.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(SpamOrHam.PLUGIN_NAME)
@Description("Trains a model based upon whether messages are spam or not.")
public final class SpamOrHam extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "SpamOrHam";
  public static final String MODEL_FILESET = "modelFileSet";
  public static final String TEXTS_TO_CLASSIFY = "textsToClassify";

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.addStream(TEXTS_TO_CLASSIFY);
    pipelineConfigurer.createDataset(MODEL_FILESET, FileSet.class, FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkPluginContext sparkContext, JavaRDD<StructuredRecord> input) throws Exception {
    Preconditions.checkArgument(input.count() != 0, "Input RDD is empty.");
    final JavaRDD<StructuredRecord> spam = input.filter(new Function<StructuredRecord, Boolean>() {
      @Override
      public Boolean call(StructuredRecord record) throws Exception {
        return record.get(SpamMessage.IS_SPAM_FIELD);
      }
    });

    JavaRDD<StructuredRecord> ham = input.filter(new Function<StructuredRecord, Boolean>() {
      @Override
      public Boolean call(StructuredRecord record) throws Exception {
        return !(Boolean) record.get(SpamMessage.IS_SPAM_FIELD);
      }
    });

    final HashingTF tf = new HashingTF(100);
    JavaRDD<Vector> spamFeatures = spam.map(new Function<StructuredRecord, Vector>() {
      @Override
      public Vector call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(SpamMessage.TEXT_FIELD);
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });

    JavaRDD<Vector> hamFeatures = ham.map(new Function<StructuredRecord, Vector>() {
      @Override
      public Vector call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(SpamMessage.TEXT_FIELD);
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });


    JavaRDD<LabeledPoint> positiveExamples = spamFeatures.map(new Function<Vector, LabeledPoint>() {
      @Override
      public LabeledPoint call(Vector vector) throws Exception {
        return new LabeledPoint(1, vector);
      }
    });

    JavaRDD<LabeledPoint> negativeExamples = hamFeatures.map(new Function<Vector, LabeledPoint>() {
      @Override
      public LabeledPoint call(Vector vector) throws Exception {
        return new LabeledPoint(0, vector);
      }
    });

    JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);
    trainingData.cache();

    final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);

    // save the model to a file in the output FileSet
    JavaSparkContext javaSparkContext = sparkContext.getOriginalSparkContext();
    FileSet outputFS = sparkContext.getDataset(MODEL_FILESET);
    model.save(JavaSparkContext.toSparkContext(javaSparkContext),
               outputFS.getBaseLocation().append("output").toURI().getPath());
  }
}
