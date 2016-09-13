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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.datastreams.DataStreamsApp;
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
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.KMeansClassifier;
import co.cask.hydrator.plugin.spark.KMeansTrainer;
import co.cask.hydrator.plugin.spark.KafkaStreamingSource;
import co.cask.hydrator.plugin.spark.NaiveBayesClassifier;
import co.cask.hydrator.plugin.spark.NaiveBayesTrainer;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Spark plugins.
 */
public class KMeansTrainerTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "3.2.0");

  private static final String CLASSIFIED_OUTPUT = "classifiedOutput";

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for data pipeline app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.toId(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT.toId(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      KMeansTrainer.class, KMeansClassifier.class, KMeansData.class, NaiveBayesTrainer.class,
                      NaiveBayesClassifier.class, KafkaStreamingSource.class, KafkaUtils.class, DefaultDecoder.class,
                      TwitterStreamingSource.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {

    // use the SparkSink to train a model
    testSinglePhaseWithSparkSink();

    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSinglePhaseWithSparkCompute();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("numbers")))
      .addStage(new ETLStage("customsink",
                             new ETLPlugin(KMeansTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                           ImmutableMap.of("path", "output.xml",
                                                           "fieldToClassify", KMeansData.TEXT_FIELD,
                                                           "predictionField", KMeansData.IS_SAME),
                                           null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);


    // set up five spam messages and five non-spam messages to be used for classification
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
//    1.2, 5.6, 3.7, 0.6, 0.1, 2.6}
    messagesToWrite.add(new KMeansData("1.2", 1.2).toStructuredRecord());
    messagesToWrite.add(new KMeansData("5.6", 5.6).toStructuredRecord());
    messagesToWrite.add(new KMeansData("3.7", 3.7).toStructuredRecord());
    messagesToWrite.add(new KMeansData("0.6", 0.6).toStructuredRecord());
    messagesToWrite.add(new KMeansData("0.1", 0.1).toStructuredRecord());
    messagesToWrite.add(new KMeansData("2.6", 2.6).toStructuredRecord());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "numbers");
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    String textsToClassify = "numbers";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(textsToClassify)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(KMeansClassifier.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("path", "output.xml",
                                                           "fieldToClassify", KMeansData.TEXT_FIELD,
                                                           "predictionField", KMeansData.IS_SAME),
                                           null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(CLASSIFIED_OUTPUT)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);


    // write some some messages to be classified
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new KMeansData("101").toStructuredRecord());
    messagesToWrite.add(new KMeansData("200").toStructuredRecord());
    messagesToWrite.add(new KMeansData("4").toStructuredRecord());
    messagesToWrite.add(new KMeansData("7").toStructuredRecord());

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, textsToClassify);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);


    DataSetManager<Table> classifiedTexts = getDataset(CLASSIFIED_OUTPUT);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(classifiedTexts);

    Set<KMeansData> results = new HashSet<>();
    for (StructuredRecord structuredRecord : structuredRecords) {
      results.add(KMeansData.fromStructuredRecord(structuredRecord));
    }
  }

}
