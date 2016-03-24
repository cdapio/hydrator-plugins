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
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkTransform;
import com.google.common.collect.Lists;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkTransform that uses a trained model to determine whether messages are spam or not.
 */
@Plugin(type = SparkTransform.PLUGIN_TYPE)
@Name(MessageClassifyingTransform.PLUGIN_NAME)
@Description("Uses a trained model to determine whether messages are spam or not.")
public class MessageClassifyingTransform extends SparkTransform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MessageClassifyingTransform.class);

  public static final String PLUGIN_NAME = "MessageClassifyingTransform";

  @Override
  public JavaRDD<StructuredRecord> transform(SparkPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    FileSet fileSet = context.getDataset(SpamOrHam.MODEL_FILESET);
    Location modelLocation = fileSet.getBaseLocation().append("output");
    if (!modelLocation.exists()) {
      LOG.warn("Failed to find model to use for classification. Location does not exist: {}.", modelLocation);
      return input;
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getOriginalSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    final NaiveBayesModel loadedModel = NaiveBayesModel.load(sparkContext, modelLocation.toURI().getPath());

    final HashingTF tf = new HashingTF(100);

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(SpamMessage.TEXT_FIELD);
        Vector vector = tf.transform(Lists.newArrayList(text.split(" ")));
        double prediction = loadedModel.predict(vector);

        return StructuredRecord.builder(structuredRecord.getSchema())
          .set(SpamMessage.TEXT_FIELD, text)
          .set(SpamMessage.IS_SPAM_FIELD, prediction >= 0.5)
          .build();
      }
    });
    return output;
  }
}
