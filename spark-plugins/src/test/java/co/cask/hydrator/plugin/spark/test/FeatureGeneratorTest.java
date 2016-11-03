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
import org.apache.spark.mllib.linalg.SparseVector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link HashingTFFeatureGenerator}.
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
}
