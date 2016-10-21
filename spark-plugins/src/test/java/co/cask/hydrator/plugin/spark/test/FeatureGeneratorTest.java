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
import co.cask.hydrator.plugin.spark.VectorUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.SparseVector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
      .put("vectorSize", "3")
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
        .set("body", "I wish Java could use case in classes").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "Logistic regression models are neat to predict data").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "Use tokenizer to convert strings into tokens").build()
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
        .set("body", "Spark").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "classes in Java").build(),
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
    List<Double> result;
    for (StructuredRecord record : structuredRecords) {
      result = (ArrayList) (record.get("result"));
      if ((Integer) record.get("offset") == 1) {
        Assert.assertArrayEquals(new double[]{-0.05517900735139847, -0.13193030655384064, 0.14834064245224},
                                 ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.3);
      } else if ((Integer) record.get("offset") == 2) {
        Assert.assertArrayEquals(new double[]{0.022059813141822815, -0.04872007171312968, -0.06246543178955714},
                                 ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.3);
      } else {
        Assert.assertArrayEquals(new double[]{0.029224658012390138, -0.04119015634059906, 0.06800720132887364},
                                 ArrayUtils.toPrimitive(result.toArray(new Double[result.size()])), 0.3);
      }
    }
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
                                                           "outputColumnMapping", "body:result",
                                                           "pattern", " "), null)))
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
    for (StructuredRecord record : structuredRecords) {
      SparseVector actual = VectorUtils.fromRecord((StructuredRecord) (record.get("result")));
      SparseVector expected;
      if ((Integer) record.get("offset") == 1) {
        expected = new SparseVector(10, new int[]{3, 6, 7, 9}, new double[]{2.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(expected, actual);
      } else if ((Integer) record.get("offset") == 2) {
        expected = new SparseVector(10, new int[]{2, 3, 4, 5, 6}, new double[]{1.0, 3.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(expected, actual);
      } else {
        expected = new SparseVector(10, new int[]{0, 2, 4, 5, 8}, new double[]{1.0, 1.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testHashingTFFeatureGeneratorWithArrayString() throws Exception {
    Schema schema = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("body", Schema.arrayOf(Schema.nullableOf(Schema.of(
                                      Schema.Type.STRING)))));

    String source = "hashing-tf-generator-arrayString-source";
    String sink = "hashing-tf-generator-arrayString-sink";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(source, schema)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(HashingTFFeatureGenerator.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("numFeatures", "10",
                                                           "outputColumnMapping", "body:result",
                                                           "pattern", " "), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sink)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("offset", 1)
        .set("body", Arrays.asList("Hi", "I", "heard", "about", "Spark")).build(),
      StructuredRecord.builder(schema)
        .set("offset", 2)
        .set("body", Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")).build(),
      StructuredRecord.builder(schema)
        .set("offset", 3)
        .set("body", new ArrayList<String>()).build()
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
    for (StructuredRecord record : structuredRecords) {
      SparseVector actual = VectorUtils.fromRecord((StructuredRecord) (record.get("result")));
      SparseVector expected;
      if ((Integer) record.get("offset") == 1) {
        expected = new SparseVector(10, new int[]{3, 6, 7, 9}, new double[]{2.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(expected, actual);
      } else if ((Integer) record.get("offset") == 2) {
        expected = new SparseVector(10, new int[]{2, 3, 4, 5, 6}, new double[]{1.0, 3.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(expected, actual);
      } else {
        expected = new SparseVector(10, new int[0], new double[0]);
        Assert.assertEquals(expected, actual);
      }
    }
  }
}
