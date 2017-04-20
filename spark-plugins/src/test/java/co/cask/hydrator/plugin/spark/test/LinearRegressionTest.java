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
import co.cask.cdap.api.data.schema.Schema;
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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.LinearRegressionPredictor;
import co.cask.hydrator.plugin.spark.LinearRegressionTrainer;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for LinearRegressionTrainer and LinearRegressionPredictor classes.
 */
public class LinearRegressionTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.5.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");
  private static final String LABELED_RECORDS = "labeledRecords";
  private final Schema schema = Schema.recordOf("input-record", Schema.Field.of("age", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("height", Schema.of(Schema.Type.DOUBLE)),
                                                Schema.Field.of("smoke", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("gender", Schema.of(Schema.Type.STRING)));

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      LinearRegressionTrainer.class, LinearRegressionPredictor.class, TwitterStreamingSource.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink(LinearRegressionTrainer) to train a model
    testSinglePhaseWithSparkSink();
    // use a SparkCompute(LinearRegressionPredictor) to label all records going through the pipeline, using the model
    // build with the SparkSink
    testSinglePhaseWithSparkCompute();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    String inputTable = "spark-model";
    /*
     * source --> sparksink
     */
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "linear-regression-model")
      .put("path", "linearRegression")
      .put("featuresToInclude", "age")
      .put("labelField", "lung_capacity")
      .put("numIterations", "50")
      .put("stepSize", "0.001")
      .build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputTable, getTrainerSchema(schema))))
      .addStage(new ETLStage("customsink", new ETLPlugin(LinearRegressionTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                                         properties, null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // send records from sample data to train the model
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.addAll(getInputData());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, inputTable);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    String inputTable = "spark-compute";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputTable, schema)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(LinearRegressionPredictor.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "linear-regression-model",
                                                           "path", "linearRegression",
                                                           "featuresToExclude", "height,smoke,gender",
                                                           "predictionField", "lung_capacity"), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(LABELED_RECORDS)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // records to be labeled
    List<StructuredRecord> messagesToWrite = ImmutableList.of(
      StructuredRecord.builder(schema).set("age", 11).set("height", 58.7).set("smoke", "no")
        .set("gender", "female").build(),
      StructuredRecord.builder(schema).set("age", 8).set("height", 63.3).set("smoke", "no")
        .set("gender", "male").build());

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, inputTable);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> labeledTexts = getDataset(LABELED_RECORDS);
    List<StructuredRecord> actualRecords = MockSink.readOutput(labeledTexts);

    for (StructuredRecord record : actualRecords) {
      if ((Integer) record.get("age") == 11) {
        Assert.assertEquals(6.225, (Double) record.get("lung_capacity"), 0.5);
      } else {
        Assert.assertEquals(4.950, (Double) record.get("lung_capacity"), 0.5);
      }
    }
  }

  //Generate data to be used for training the model.
  private List<StructuredRecord> getInputData() throws IOException {
    Schema trainerSchema = getTrainerSchema(schema);
    List<StructuredRecord> messagesToWrite = ImmutableList.of(
      StructuredRecord.builder(trainerSchema).set("age", 6).set("height", 62.1).set("smoke", "no")
        .set("gender", "male").set("lung_capacity", 6.475).build(),
      StructuredRecord.builder(trainerSchema).set("age", 18).set("height", 74.7).set("smoke", "yes")
        .set("gender", "female").set("lung_capacity", 10.125).build(),
      StructuredRecord.builder(trainerSchema).set("age", 16).set("height", 69.7).set("smoke", "no")
        .set("gender", "female").set("lung_capacity", 9.550).build(),
      StructuredRecord.builder(trainerSchema).set("age", 14).set("height", 71.0).set("smoke", "no")
        .set("gender", "male").set("lung_capacity", 11.125).build(),
      StructuredRecord.builder(trainerSchema).set("age", 5).set("height", 56.9).set("smoke", "no")
        .set("gender", "male").set("lung_capacity", 4.800).build());

    return messagesToWrite;
  }

  private Schema getTrainerSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of("lung_capacity", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    return Schema.recordOf(schema.getRecordName() + ".predicted", fields);
  }
}
