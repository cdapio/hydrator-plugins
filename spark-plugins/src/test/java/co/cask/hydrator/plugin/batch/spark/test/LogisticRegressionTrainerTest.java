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

package co.cask.hydrator.plugin.batch.spark.test;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkSink;
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
import co.cask.hydrator.plugin.batch.spark.LogisticRegressionTrainer;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Logistic Regression Spark plugin.
 */

public class LogisticRegressionTrainerTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      LogisticRegressionTrainer.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink to train a model using Logistic Regression
    testSinglePhaseWithSparkSink();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "modelFileSet")
      .put("path", "output")
      .put("fieldsToClassify",
           LogisticRegressionSpamMessageModel.TEXT_FIELD + ", " + LogisticRegressionSpamMessageModel.READ_FIELD)
      .put("predictionField", LogisticRegressionSpamMessageModel.SPAM_PREDICTION_FIELD)
      .put("numFeatures", LogisticRegressionSpamMessageModel.SPAM_FEATURES)
      .put("numClasses", "10")
      .build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("messages")))
      .addStage(new ETLStage("customsink", new ETLPlugin(LogisticRegressionTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                                         sourceProperties, null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set up five spam messages and five non-spam messages to be used for classification
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("buy our clothes", "yes", 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("sell your used books to us", "yes", 1.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("earn money for free", "yes", 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is definitely not spam", "yes", 1.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("you won the lottery", "yes", 1.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("how was your day", "no", 0.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("what are you up to", "no", 0.0).toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is a genuine message", "no", 0.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("this is an even more genuine message", "no", 0.0)
                          .toStructuredRecord());
    messagesToWrite.add(new LogisticRegressionSpamMessageModel("could you send me the report", "no", 0.0)
                          .toStructuredRecord());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "messages");
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

}
