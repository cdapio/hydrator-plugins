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
import co.cask.hydrator.plugin.spark.HashingTFFeatureGenerator;
import co.cask.hydrator.plugin.spark.SkipGramFeatureGenerator;
import co.cask.hydrator.plugin.spark.SkipGramTrainer;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@Link SkipGramTrainer} , {@link SkipGramFeatureGenerator}, {@link HashingTFFeatureGenerator}.
 */
public class FeatureGeneratorTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.5.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));


  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      SkipGramTrainer.class, SkipGramFeatureGenerator.class, HashingTFFeatureGenerator.class,
                      TwitterStreamingSource.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink(SkipGramTrainer) to train a model
    testSkipGramTrainer();
    // use a SparkCompute(SkipGramFeatureGenerator) to generate text based features using the saved model.
    testSkipGramFeatureGenerator();
  }

  private void testSkipGramTrainer() throws Exception {
    String source = "skip-gram-trainer";
    /*
     * source --> sparksink
     */
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "training-model")
      .put("path", "Model")
      .put("inputCol", "body")
      .build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(source, INPUT)))
      .addStage(new ETLStage("sink", new ETLPlugin(SkipGramTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                                         properties, null)))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // send records from sample data to train the model
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "Hi I heard about Spark").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "I wish Java could use case classes").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "Logistic regression models are neat").build()
    );

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, source);
    MockSource.writeInput(inputManager, input);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void testSkipGramFeatureGenerator() throws Exception {
    String source = "skip-gram-feature-generator-source";
    String sink = "skip-gram-feature-generator-sink";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(source, INPUT)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(SkipGramFeatureGenerator.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "training-model",
                                                           "path", "Model",
                                                           "outputColumnMapping", "body:result"), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sink)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "Spark ML plugins").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "Classes in Java").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "Logistic regression to predict data").build()
    );

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, source);
    MockSource.writeInput(inputManager, input);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> labeledTexts = getDataset(sink);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(labeledTexts);
    Assert.assertEquals(3, structuredRecords.size());
    List<Double> result = (ArrayList) (structuredRecords.get(0).get("result"));
    Assert.assertArrayEquals(new double[]{0.040902843077977494, -0.010430609186490376, -0.04750693837801615},
                             ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.1);
    result = (ArrayList) (structuredRecords.get(1).get("result"));
    Assert.assertArrayEquals(new double[]{-0.04352385476231575, 3.2448768615722656E-4, 0.02223073500208557},
                             ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.1);
    result = (ArrayList) (structuredRecords.get(2).get("result"));
    Assert.assertArrayEquals(new double[]{0.011901815732320149, 0.019348077476024628, -0.0074237411220868426},
                             ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.1);
  }

  @Test
  public void testHashingTFFeatureGenerator() throws Exception {
    String source = "hashing-tf-generator-source";
    String sink = "hashing-tf-generator-sink";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(source, INPUT)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(HashingTFFeatureGenerator.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("numFeatures", "10",
                                                           "outputColumnMapping", "body:result"), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sink)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "Hi I heard about Spark").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "I wish Java could use case classes").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "Logistic regression models are neat").build()
    );

    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, source);
    MockSource.writeInput(inputManager, input);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> labeledTexts = getDataset(sink);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(labeledTexts);
    Assert.assertEquals(3, structuredRecords.size());
    List<Double> indices;
    List<Double> values;
    for (StructuredRecord record : structuredRecords) {
      indices = (ArrayList) (record.get("result_indices"));
      values = (ArrayList) (record.get("result_value"));
      if ((Integer) record.get("offset") == 1) {

        Assert.assertArrayEquals(new int[]{3, 6, 7, 9},
                                 ArrayUtils.toPrimitive(indices.toArray(new Integer[indices.size()])));
        Assert.assertArrayEquals(new double[]{2.0, 1.0, 1.0, 1.0},
                                 ArrayUtils.toPrimitive(values.toArray(new Double[values.size()])), 0.1);
      } else if ((Integer) record.get("offset") == 2) {
        Assert.assertArrayEquals(new int[]{2, 3, 4, 5, 6},
                                 ArrayUtils.toPrimitive(indices.toArray(new Integer[indices.size()])));
        Assert.assertArrayEquals(new double[]{1.0, 3.0, 1.0, 1.0, 1.0},
                                 ArrayUtils.toPrimitive(values.toArray(new Double[values.size()])), 0.1);
      } else {
        Assert.assertArrayEquals(new int[]{0, 2, 4, 5, 8},
                                 ArrayUtils.toPrimitive(indices.toArray(new Integer[indices.size()])));
        Assert.assertArrayEquals(new double[]{1.0, 1.0, 1.0, 1.0, 1.0},
                                 ArrayUtils.toPrimitive(values.toArray(new Double[values.size()])), 0.1);
      }
    }
  }
}
