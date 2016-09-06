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

package co.cask.hydrator.plugin.spark.test;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.LogisticRegressionClassifier;
import co.cask.hydrator.plugin.spark.LogisticRegressionTrainer;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Logistic Regression Spark plugin.
 */
public class LogisticRegressionTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static final String CLASSIFIED_TEXTS = "classifiedTexts";

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      LogisticRegressionTrainer.class, LogisticRegressionClassifier.class,
                      TwitterStreamingSource.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink to train a model using Logistic Regression
    testSinglePhaseWithSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSinglePhaseWithSparkCompute();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    String fieldsToClassify =
      LogisticRegressionSpamMessageModel.TEXT_FIELD + "," + LogisticRegressionSpamMessageModel.READ_FIELD;
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "modelFileSet")
      .put("path", "output")
      .put("featureFields", fieldsToClassify)
      .put("labelField", LogisticRegressionSpamMessageModel.SPAM_PREDICTION_FIELD)
      .put("numFeatures", LogisticRegressionSpamMessageModel.SPAM_FEATURES)
      .put("numClasses", "2")
      .build();

    ETLPlugin sink = new ETLPlugin(LogisticRegressionTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                   sourceProperties, null);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("messages")))
      .addStage(new ETLStage("customsink", sink))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set up five spam messages and five non-spam messages to be used for classification
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("buy our clothes", 0.0, 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("sell your used books to us", 0.0, 1.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("earn money for free", 0.0, 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is definitely not spam", 0.0, 1.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("you won the lottery", 0.0, 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("how was your day", 1.0, 0.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("what are you up to", 1.0, 0.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is a genuine message", 1.0, 0.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is an even more genuine message", 1.0, 0.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("could you send me the report", 1.0, 0.0)
                          .toStructuredRecord());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "messages");
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    String textsToClassify = "textsToClassify";
    /*
     * source --> sparkcompute --> sink
     */
    String fieldsToClassify =
      LogisticRegressionSpamMessageModel.TEXT_FIELD + "," + LogisticRegressionSpamMessageModel.READ_FIELD;
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "modelFileSet")
      .put("path", "output")
      .put("fieldsToClassify", fieldsToClassify)
      .put("predictionField", LogisticRegressionSpamMessageModel.SPAM_PREDICTION_FIELD)
      .put("numFeatures", LogisticRegressionSpamMessageModel.SPAM_FEATURES)
      .build();

    ETLPlugin sink = new ETLPlugin(LogisticRegressionClassifier.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                   sourceProperties, null);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(textsToClassify)))
      .addStage(new ETLStage("sparkcompute", sink))
      .addStage(new ETLStage("sink", MockSink.getPlugin(CLASSIFIED_TEXTS)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write some some messages to be classified
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("how are you doing today", 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("earn money", 0.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("what are you doing today", 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("genuine report", 1.0).toStructuredRecord());

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, textsToClassify);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> classifiedTexts = getDataset(CLASSIFIED_TEXTS);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(classifiedTexts);

    Set<LogisticRegressionSpamMessageModel> results = new HashSet<>();
    for (StructuredRecord structuredRecord : structuredRecords) {
      results.add(LogisticRegressionSpamMessageModel.fromStructuredRecord(structuredRecord));
    }

    Set<LogisticRegressionSpamMessageModel> expected = new HashSet<>();
    expected.add(new LogisticRegressionSpamMessageModel("how are you doing today", 1.0, 0.0));
    // only 'earn money' should be predicated as spam
    expected.add(new LogisticRegressionSpamMessageModel("earn money", 0.0, 1.0));
    expected.add(new LogisticRegressionSpamMessageModel("what are you doing today", 1.0, 0.0));
    expected.add(new LogisticRegressionSpamMessageModel("genuine report", 1.0, 0.0));

    Assert.assertEquals(expected, results);
  }
}
