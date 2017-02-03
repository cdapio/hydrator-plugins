/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests to verify configuration of {@link FileBatchSource}
 */
public class FileBatchSourceTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary BATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "4.0.0"), BATCH_APP_ARTIFACT_ID,
                      FileBatchSource.class);
  }

  @Test
  public void testDefaults() {
    FileBatchSource.FileBatchConfig config = new FileBatchSource.FileBatchConfig();
    FileBatchSource fileBatchSource = new FileBatchSource(config);
    FileBatchSource.FileBatchConfig fileBatchConfig = fileBatchSource.getConfig();
    Assert.assertEquals(new Gson().toJson(ImmutableMap.<String, String>of()), fileBatchConfig.fileSystemProperties);
    Assert.assertEquals(".*", fileBatchConfig.fileRegex);
    Assert.assertEquals(CombineTextInputFormat.class.getName(), fileBatchConfig.inputFormatClass);
    Assert.assertNotNull(fileBatchConfig.maxSplitSize);
    Assert.assertEquals(FileBatchSource.DEFAULT_MAX_SPLIT_SIZE, (long) fileBatchConfig.maxSplitSize);
  }

  @Test
  public void testIgnoreNonExistingFolder() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "/src/test/resources/path_one/")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "true")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "ignore-non-existing-files";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 0, output.size());
  }

  @Test
  public void testNotPresentFolder() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "/src/test/resources/path_one/")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("InvalidFileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, 1, 5, TimeUnit.MINUTES);
  }

  @Test
  public void testRecursiveFolders() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/")
      .put(Properties.File.FILE_REGEX, "[a-zA-Z0-9\\-:/_]*/x/[a-z0-9]*.txt$")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "true")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "recursive-folders";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add((String) record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Test
  public void testNonRecursiveRegex() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/")
      .put(Properties.File.FILE_REGEX, ".+fileBatchSource.*")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "false")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "non-recursive-regex";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 1, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add((String) record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Test
  public void testFileRegex() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/test1/x/")
      .put(Properties.File.FILE_REGEX, ".+test.*")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "false")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "file-regex";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 1, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add((String) record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }


  @Test
  public void testRecursiveRegex() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/")
      .put(Properties.File.FILE_REGEX, ".+fileBatchSource.*")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "true")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "recursive-regex";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add((String) record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Test
  public void testPathGlobbing() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/*/x/")
      .put(Properties.File.FILE_REGEX, ".+.txt")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "false")
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "path-globbing";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest");

    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add((String) record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }
}
