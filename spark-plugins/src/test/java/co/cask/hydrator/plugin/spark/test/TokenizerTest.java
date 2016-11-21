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
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
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
import co.cask.hydrator.plugin.spark.Tokenizer;
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
 * Test for Tokenizer plugin.
 */
public class TokenizerTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.5.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");

  private static final String SINGLE_COLUMN_DATASET = "SingleColumn";
  private static final String COMMA_DATASET = "commaDataset";
  private static final String SPACE_DATASET = "spaceDataset";
  private static final String MULTIPLE_SPACE_DATASET = "multiSpaceDataset";
  private static final String TAB_DATASET = "tabDataset";
  private static final String OUTPUT_COLUMN = "words";
  private static final String COLUMN_TOKENIZED = "sentence";
  private static final String PATTERN = "/";
  private static final String NAME_COLUMN = "name";
  private static final String APP_NAME = "TokenizerTest";
  private static final String SENTENCE1 = "cask data /application platform";
  private static final String SENTENCE2 = "cask hydrator/ is webbased tool";
  private static final String SENTENCE3 = "hydrator studio is visual /development environment";
  private static final String SENTENCE4 = "hydrator plugins /are customizable modules";
  private static final String SENTENCE_WITH_SPACE = "cask data application platform";
  private static final String SENTENCE_WITH_MULTIPLE_SPACE = "cask data  application  platform";
  private static final String SENTENCE_WITH_COMMA = "cask,data,application,platform";
  private static final String SENTENCE_WITH_TAB = "cask    data    application platform";

  private static final Schema SOURCE_SCHEMA_SINGLE = Schema.recordOf("sourceRecord",
                                                                     Schema.Field.of(COLUMN_TOKENIZED,
                                                                                     Schema.of(Schema.Type.STRING)));
  private static final Schema SOURCE_SCHEMA_MULTIPLE =
    Schema.recordOf("sourceRecord", Schema.Field.of(NAME_COLUMN, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(COLUMN_TOKENIZED, Schema.of(Schema.Type.STRING)));

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.toId(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true));
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      Tokenizer.class, TwitterStreamingSource.class);
  }

  /**
   * @param mockNameOfSourcePlugin used while adding ETLStage for mock source
   * @param mockNameOfSinkPlugin used while adding ETLStage for mock sink
   * @param pattern config parameter for Tokenizer plugin
   * @return ETLBatchConfig
   */
  private ETLBatchConfig buildETLBatchConfig(String mockNameOfSourcePlugin,
                                             String mockNameOfSinkPlugin, String pattern) {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(mockNameOfSourcePlugin)))
      .addStage(new ETLStage("sparkcompute", new ETLPlugin(Tokenizer.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("outputColumn", OUTPUT_COLUMN,
                                                           "columnToBeTokenized", COLUMN_TOKENIZED,
                                                           "patternSeparator", pattern), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(mockNameOfSinkPlugin)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();
    return etlConfig;
  }

  @Test
  public void testMultiColumnSource() throws Exception {
    String mockSource = "textForMultiple";
    String multiColumnData = "MultipleColumns";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, multiColumnData, PATTERN);
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE1).set(NAME_COLUMN, "CDAP")
        .build(),
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE2).set(NAME_COLUMN, "Hydrator")
        .build(),
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE3).set(NAME_COLUMN, "Studio")
        .build(),
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE4).set(NAME_COLUMN, "Plugins")
        .build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTexts = getDataset(multiColumnData);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTexts);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_COLUMN));
    }
    //Create expected data
    Set<List<String>> expected = getExpectedData();
    Assert.assertEquals(expected, results);
    Assert.assertEquals(4, output.size());
    StructuredRecord row = output.get(0);
    Schema expectedSchema = Schema.recordOf("record", Schema.Field.of(NAME_COLUMN, Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of(COLUMN_TOKENIZED, Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of(OUTPUT_COLUMN,
                                                            Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    Assert.assertEquals("Schema", expectedSchema, row.getSchema());
    Assert.assertEquals(3, row.getSchema().getFields().size());
    Assert.assertNotNull(row.getSchema().getField(COLUMN_TOKENIZED));
    Assert.assertNotNull(row.getSchema().getField(NAME_COLUMN));
    Assert.assertEquals("ARRAY", row.getSchema().getField(OUTPUT_COLUMN).getSchema().getType().toString());
  }

  @Test
  public void testSingleColumnSource() throws Exception {
    String mockSource = "Single";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, SINGLE_COLUMN_DATASET, PATTERN);
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(COLUMN_TOKENIZED, SENTENCE1).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(COLUMN_TOKENIZED, SENTENCE2).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(COLUMN_TOKENIZED, SENTENCE3).build(),
      StructuredRecord.builder(SOURCE_SCHEMA_SINGLE).set(COLUMN_TOKENIZED, SENTENCE4).build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTextSingle = getDataset(SINGLE_COLUMN_DATASET);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTextSingle);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_COLUMN));
    }
    //Create expected data
    Set<List<String>> expected = getExpectedData();
    Assert.assertEquals(expected, results);
    Assert.assertEquals(4, output.size());
    StructuredRecord rowSingle = output.get(0);
    Schema expectedSchema = Schema.recordOf("record", Schema.Field.of(COLUMN_TOKENIZED, Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of(OUTPUT_COLUMN,
                                                            Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    Assert.assertEquals("Schema", expectedSchema, rowSingle.getSchema());
    Assert.assertEquals(2, rowSingle.getSchema().getFields().size());
    Assert.assertNotNull(rowSingle.getSchema().getField(COLUMN_TOKENIZED));
    Assert.assertEquals("ARRAY", rowSingle.getSchema().getField(OUTPUT_COLUMN).getSchema().getType().toString());
  }

  @Test(expected = NullPointerException.class)
  public void testNullDelimiter() throws Exception {
    buildETLBatchConfig("NegativeTestForDelimiter", SINGLE_COLUMN_DATASET, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckWrongArgument() throws Exception {
    Schema sourceSchema = Schema.recordOf("sourceRecord",
                                          Schema.Field.of(NAME_COLUMN, Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of(COLUMN_TOKENIZED, Schema.of(Schema.Type.INT)));
    Tokenizer.Config config = new Tokenizer.Config("output", COLUMN_TOKENIZED, "/");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(sourceSchema);
    new Tokenizer(config).configurePipeline(configurer);
  }

  @Test
  public void testDelimiters() throws Exception {
    testDelimiter("textForComma", COMMA_DATASET, SENTENCE_WITH_COMMA, ",");
    testDelimiter("textForSpace", SPACE_DATASET, SENTENCE_WITH_SPACE, " ");
    testDelimiter("textForTab", TAB_DATASET, SENTENCE_WITH_TAB, " ");
    testDelimiter("textForMultipleSpaces", MULTIPLE_SPACE_DATASET, SENTENCE_WITH_MULTIPLE_SPACE, "\\s+");
  }

  private void testDelimiter(String mockSource, String mockSink, String sentence, String delimiter) throws Exception {
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = buildETLBatchConfig(mockSource, mockSink, delimiter);
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    DataSetManager<Table> inputManager = getDataset(mockSource);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED,
                                                           sentence).set(NAME_COLUMN, "CDAP").build());
    MockSource.writeInput(inputManager, input);
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    DataSetManager<Table> tokenizedTexts = getDataset(mockSink);
    List<StructuredRecord> output = MockSink.readOutput(tokenizedTexts);
    Set<List> results = new HashSet<>();
    for (StructuredRecord structuredRecord : output) {
      results.add((ArrayList) structuredRecord.get(OUTPUT_COLUMN));
    }
    //Create expected data
    Set<List<String>> expected = getExpectedDataForSingleSentence();
    Assert.assertEquals(expected, results);
    Assert.assertEquals(1, output.size());
    StructuredRecord row = output.get(0);
    Assert.assertEquals(3, row.getSchema().getFields().size());
    Assert.assertEquals("ARRAY", row.getSchema().getField(OUTPUT_COLUMN).getSchema().getType().toString());
  }

  private Set<List<String>> getExpectedData() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(Arrays.asList("cask data ", "application platform"));
    expected.add(Arrays.asList("cask hydrator", " is webbased tool"));
    expected.add(Arrays.asList("hydrator studio is visual ", "development environment"));
    expected.add(Arrays.asList("hydrator plugins ", "are customizable modules"));
    return expected;
  }

  private Set<List<String>> getExpectedDataForSingleSentence() {
    Set<List<String>> expected = new HashSet<>();
    expected.add(Arrays.asList("cask", "data", "application", "platform"));
    return expected;
  }
}
