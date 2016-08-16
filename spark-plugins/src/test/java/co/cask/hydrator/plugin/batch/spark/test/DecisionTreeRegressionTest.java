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
import co.cask.hydrator.plugin.batch.spark.DecisionTreeRegressor;
import co.cask.hydrator.plugin.batch.spark.DecisionTreeTrainer;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@Link DecisionTreeTrainer} and {@link DecisionTreeRegressor} classes.
 */
public class DecisionTreeRegressionTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  private static final String LABELED_RECORDS = "labeledRecords";

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      DecisionTreeTrainer.class, DecisionTreeRegressor.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink(DecisionTreeTrainer) to train a model
    testSinglePhaseWithSparkSink();
    // use a SparkCompute(DecisionTreeRegressor) to label all records going through the pipeline, using the model
    // build with the SparkSink
    testSinglePhaseWithSparkCompute();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "decision-tree-regression-model")
      .put("path", "decisionTreeRegression")
      .put("features", "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,origin,dest")
      .put("predictionField", "delayed")
      .put("maxBins", "100")
      .put("maxDepth", "9")
      .build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("flightRecords")))
      .addStage(new ETLStage("customsink", new ETLPlugin(DecisionTreeTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                                         properties, null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // send records from sample data to train the model
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.addAll(getInputData());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "flightRecords");
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  //Get data from file to be used for training the model.
  private List<StructuredRecord> getInputData() throws IOException {
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    File file = new File(this.getClass().getResource("/trainData.csv").getFile());
    BufferedReader bufferedInputStream = new BufferedReader(new FileReader(file));
    String line;
    while ((line = bufferedInputStream.readLine()) != null) {
      String[] flightData = line.split(",");
      Double depdelaymins = Double.parseDouble(flightData[11]);
      //For binary classification create delayed field containing values 1.0 and 0.0 depending on the delay time.
      double delayed = depdelaymins > 40 ? 1.0 : 0.0;
      messagesToWrite.add(new Flight(Integer.parseInt(flightData[0]), Integer.parseInt(flightData[1]), flightData[2],
                                     flightData[3],
                                     Integer.parseInt(flightData[4]), flightData[5], flightData[6], flightData[7],
                                     flightData[8], Integer.parseInt(flightData[9]), Double.parseDouble(flightData[10]),
                                     depdelaymins, Double.parseDouble(flightData[12]),
                                     Double.parseDouble(flightData[13]), Double.parseDouble(flightData[14]),
                                     Double.parseDouble(flightData[15]), Integer.parseInt(flightData[16]), delayed)
                            .toStructuredRecord());
    }
    return messagesToWrite;
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    String fetaures = "fetaures";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(fetaures)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(DecisionTreeRegressor.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "decision-tree-regression-model",
                                                           "path", "decisionTreeRegression",
                                                           "features", "dofM,dofW,scheduleDepTime,scheduledArrTime," +
                                                             "carrier,elapsedTime,origin,dest",
                                                           "predictionField", "delayed"),
                                           null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(LABELED_RECORDS)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // Flight records to be labeled.
    Set<StructuredRecord> messagesToWrite = new LinkedHashSet<>();
    messagesToWrite.add(new Flight(4, 6, "AA", "N327AA", 1, "12478", "JFK", "12892", "LAX", 900, 1005.0, 65.0,
                                   1225.0, 1324.0, 59.0, 385.0, 2475).toStructuredRecord());
    messagesToWrite.add(new Flight(25, 6, "MQ", "N0EGMQ", 3419, "10397", "ATL", "12953", "LGA", 1150, 1229.0, 39.0,
                                   1359.0, 1448.0, 49.0, 129.0, 762).toStructuredRecord());
    messagesToWrite.add(new Flight(4, 6, "EV", "N14991", 6159, "13930", "ORD", "13198", "MCI", 2030, 2118.0, 48.0,
                                   2205.0, 2321.0, 76.0, 95.0, 403).toStructuredRecord());
    messagesToWrite.add(new Flight(29, 3, "AA", "N355AA", 2407, "12892", "LAX", "11298", "DFW", 1025, 1023.0, 0.0,
                                   1530.0, 1523.0, 0.0, 185.0, 1235).toStructuredRecord());
    messagesToWrite.add(new Flight(2, 4, "DL", "N919DE", 1908, "13930", "ORD", "11433", "DTW", 1641, 1902.0, 141.0,
                                   1905.0, 2117.0, 132.0, 84.0, 235).toStructuredRecord());
    messagesToWrite.add(new Flight(2, 4, "DL", "N933DN", 1791, "10397", "ATL", "15376", "TUS", 1855, 2014.0, 79.0,
                                   2108.0, 2159.0, 51.0, 253.0, 1541).toStructuredRecord());

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, fetaures);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> labeledTexts = getDataset(LABELED_RECORDS);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(labeledTexts);

    Set<Flight> results = new LinkedHashSet<>();
    for (StructuredRecord structuredRecord : structuredRecords) {
      results.add(Flight.fromStructuredRecord(structuredRecord));
    }

    Set<Flight> expected = new HashSet<>();
    expected.add(new Flight(4, 6, "AA", "N327AA", 1, "12478", "JFK", "12892", "LAX", 900, 1005.0, 65.0, 1225.0,
                            1324.0, 59.0, 385.0, 2475, 1.0));
    expected.add(new Flight(29, 3, "AA", "N355AA", 2407, "12892", "LAX", "11298", "DFW", 1025, 1023.0, 0.0, 1530.0,
                            1523.0, 0.0, 185.0, 1235, 0.0));
    expected.add(new Flight(4, 6, "EV", "N14991", 6159, "13930", "ORD", "13198", "MCI", 2030, 2118.0, 48.0, 2205.0,
                            2321.0, 76.0, 95.0, 403, 1.0));
    expected.add(new Flight(25, 6, "MQ", "N0EGMQ", 3419, "10397", "ATL", "12953", "LGA", 1150, 1229.0, 39.0, 1359.0,
                            1448.0, 49.0, 129.0, 762, 0.0));
    expected.add(new Flight(2, 4, "DL", "N919DE", 1908, "13930", "ORD", "11433", "DTW", 1641, 1902.0, 141.0, 1905.0,
                            2117.0, 132.0, 84.0, 235, 1.0));
    expected.add(new Flight(2, 4, "DL", "N933DN", 1791, "10397", "ATL", "15376", "TUS", 1855, 2014.0, 79.0, 2108.0,
                            2159.0, 51.0, 253.0, 1541, 1.0));
    Assert.assertEquals(expected, results);
  }
}
