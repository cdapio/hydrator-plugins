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
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.NGramTransform;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for NGramTransform plugin.
 */
public class NGramTransformTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.5.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");
  private static final String APP_NAME = "NGramTransformTest";
  private static final String OUTPUT_FIELD = "ngrams";
  private static final String FIELD_TO_BE_TRANSFORMED = "tokens";
  private static final String ADDITIONAL_FIELD = "name";
  private static final String[] DATA_TO_BE_TRANSFORMED1 = {"hi", "i", "am", "cdap"};
  private static final String[] DATA_TO_BE_TRANSFORMED2 = {"how", "are", "you", "cdap"};
  private static final String[] DATA_TO_BE_TRANSFORMED3 = {"hi", "i"};
  private static final String SINGLE_FIELD_DATASET = "SingleField";

  private static final Schema SOURCE_SCHEMA_SINGLE =
    Schema.recordOf("sourceRecord", Schema.Field.of(FIELD_TO_BE_TRANSFORMED,
                                                    Schema.arrayOf(Schema.of(Schema.Type.STRING))));

  private static final Schema SOURCE_SCHEMA_MULTIPLE =
    Schema.recordOf("sourceRecord", Schema.Field.of(ADDITIONAL_FIELD, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(FIELD_TO_BE_TRANSFORMED, Schema.arrayOf(Schema.of(Schema.Type.STRING))));

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.toId(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      NGramTransform.class, TwitterStreamingSource.class);
  }

  /**
   * @param mockNameOfSourcePlugin used while adding ETLStage for mock source
   * @param mockNameOfSinkPlugin used while adding ETLStage for mock sink
   * @param ngramSize size of NGram
   * @return ETLBatchConfig
   */
  private ETLBatchConfig buildETLBatchConfig(String mockNameOfSourcePlugin,
                                             String mockNameOfSinkPlugin, String ngramSize) {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(mockNameOfSourcePlugin)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(NGramTransform.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("outputField", OUTPUT_FIELD,
                                                           "fieldToBeTransformed", FIELD_TO_BE_TRANSFORMED,
                                                           "ngramSize", ngramSize),
                                           null))).addStage(new ETLStage("sink",
                                                                         MockSink.getPlugin(mockNameOfSinkPlugin)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();
    return etlConfig;
  }

  @Test
  public void testMultiFieldsSourceWith2N() throws Exception {
    String mockSource = "Multiple";
    String multiFieldData = "MultipleFields";
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, multiFieldData, "2");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED1)
        .set(ADDITIONAL_FIELD, "CDAP1").build(),
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED2)
        .set(ADDITIONAL_FIELD, "CDAP2").build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> nGrams = getDataset(multiFieldData);
    List<StructuredRecord> output = MockSink.readOutput(nGrams);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_FIELD));
    }
    Assert.assertEquals(2, output.size());
    Assert.assertEquals(getExpectedData(), results);
    StructuredRecord row = output.get(0);
    Assert.assertEquals("ARRAY", row.getSchema().getField(OUTPUT_FIELD).getSchema().getType().toString());
  }

  @Test
  public void testSingleFieldSourceWith3N() throws Exception {
    String mockSource = "Single";
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, SINGLE_FIELD_DATASET, "3");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED1).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED2).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(FIELD_TO_BE_TRANSFORMED, DATA_TO_BE_TRANSFORMED3).build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTextSingle = getDataset(SINGLE_FIELD_DATASET);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_FIELD));
    }
    Assert.assertEquals(getExpectedDataFor3N(), results);
    Assert.assertEquals("ARRAY", output.get(0).getSchema().getField(OUTPUT_FIELD).
      getSchema().getType().toString());
  }

  @Test
  public void testStringFieldSource() throws Exception {
    String mockSource = "StringField";
    String mockSink = "sink";
    Schema inputSchema = Schema.recordOf("sourceRecord",
                                         Schema.Field.of(FIELD_TO_BE_TRANSFORMED, Schema.of(Schema.Type.STRING)));
    /*
     * source --> sparkcompute --> sink
    */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, mockSink, "2");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("test");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set(FIELD_TO_BE_TRANSFORMED, "hi i am cdap").build(),
      StructuredRecord.builder(inputSchema).set(FIELD_TO_BE_TRANSFORMED, "how are you cdap").build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTextSingle = getDataset(mockSink);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_FIELD));
    }
    Assert.assertEquals(getExpectedData(), results);
    Assert.assertEquals("ARRAY", output.get(0).getSchema().getField(OUTPUT_FIELD).
      getSchema().getType().toString());
  }

  @Test(expected = NullPointerException.class)
  public void testNullNGramSize() throws Exception {
    buildETLBatchConfig("NegativeTestFornGramSize", SINGLE_FIELD_DATASET, null);
  }

  private Set<List<String>> getExpectedData() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(Arrays.asList("hi i", "i am", "am cdap"));
    expected.add(Arrays.asList("how are", "are you", "you cdap"));
    return expected;
  }

  private Set<List<String>> getExpectedDataFor3N() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(new ArrayList<String>());
    expected.add(Arrays.asList("hi i am", "i am cdap"));
    expected.add(Arrays.asList("how are you", "are you cdap"));
    return expected;
  }
}
