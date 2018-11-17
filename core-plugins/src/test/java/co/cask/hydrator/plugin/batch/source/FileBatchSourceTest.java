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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.metadata.MetadataAdmin;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

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
  private static final Schema RECORD_SCHEMA = Schema.recordOf("record",
                                                              Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                              Schema.Field.of("l", Schema.of(Schema.Type.LONG)),
                                                              Schema.Field.of("file",
                                                                              Schema.of(Schema.Type.STRING)));
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  private static String fileName = dateFormat.format(new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)));
  private static File file1;
  private static File file2;
  private static MetadataAdmin metadataAdmin;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "4.0.0"), BATCH_APP_ARTIFACT_ID,
                      FileBatchSource.class);

    file1 = temporaryFolder.newFolder("test").toPath().resolve(fileName + "-test1.txt").toFile();
    FileUtils.writeStringToFile(file1, "Hello,World");
    file2 = temporaryFolder.newFile(fileName + "-test2.txt");
    FileUtils.writeStringToFile(file2, "CDAP,Platform");
    metadataAdmin = getMetadataAdmin();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (file1.exists()) {
      file1.delete();
    }
    if (file2.exists()) {
      file2.delete();
    }
    temporaryFolder.delete();
  }

  @Ignore
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-ignore-non-existing-files");

    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-not-present-folder");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, 1, 5, TimeUnit.MINUTES);
  }

  @Ignore
  @Test
  public void testRecursiveFolders() throws Exception {
    Schema schema = Schema.recordOf("file.record",
                                    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, "src/test/resources/")
      .put(Properties.File.FILE_REGEX, "[a-zA-Z0-9\\-:/_]*/x/[a-z0-9]*.txt$")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "true")
      .put("pathField", "file")
      .put("filenameOnly", "true")
      .put(Properties.File.SCHEMA, schema.toString())
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "recursive-folders";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-recursive-folders");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);

    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(schema).set("offset", 0L).set("body", "Hello,World").set("file", "test1.txt").build(),
      StructuredRecord.builder(schema).set("offset", 0L).set("body", "CDAP,Platform").set("file", "test3.txt").build());
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputManager));
    Assert.assertEquals(expected, actual);
  }

  @Ignore
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-non-recursive-regex");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 1, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Ignore
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-file-Regex");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 1, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Ignore
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-recursive-regex");

    ApplicationManager appManager = deployApplication(appId, appRequest);
    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }


  @Ignore
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-path-globbing");

    ApplicationManager appManager = deployApplication(appId, appRequest);
    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Ignore
  @Test
  public void testCopyHeader() throws Exception {
    File inputFile = temporaryFolder.newFile();

    try (Writer writer = new FileWriter(inputFile)) {
      writer.write("header123\n");
      writer.write("123456789\n");
      writer.write("987654321\n");
    }

    // each line is 10 bytes. So a max split size of 10 should break the file into 3 splits.
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "CopyHeader")
      .put("copyHeader", "true")
      .put("maxSplitSize", "10")
      .put(Properties.File.PATH, inputFile.getAbsolutePath())
      .build();

    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "copyHeaderOutput";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("CopyHeaderTest");

    ApplicationManager appManager = deployApplication(appId, appRequest);
    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);

    Map<String, Integer> expected = new HashMap<>();
    expected.put("header123", 3);
    expected.put("123456789", 1);
    expected.put("987654321", 1);
    Map<String, Integer> actual = new HashMap<>();
    for (StructuredRecord record : MockSink.readOutput(outputManager)) {
      String body = record.get("body");
      if (actual.containsKey(body)) {
        actual.put(body, actual.get(body) + 1);
      } else {
        actual.put(body, 1);
      }
    }
    Assert.assertEquals(expected, actual);
  }

  @Ignore
  @Test
  public void testTimeFilterRegex() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, file1.getParent().replaceAll("\\\\", "/"))
      .put(Properties.File.FILE_REGEX, "timefilter")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "false")
      .build();
    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));
    String outputDatasetName = "time-filter";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-timefilter-regex");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals("Expected records", 1, output.size());
    Assert.assertEquals("Hello,World", output.get(0).get("body"));
  }

  @Ignore
  @Test
  public void testRecursiveTimeFilterRegex() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put(Properties.File.PATH, file1.getParentFile().getParent().replaceAll("\\\\", "/"))
      .put(Properties.File.FILE_REGEX, "timefilter")
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put(Properties.File.RECURSIVE, "true")
      .build();
    ETLStage source = new ETLStage("FileInput", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties, null));
    String outputDatasetName = "recursive-timefilter-regex";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-recursive-timefilter-regex");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello,World"));
    Assert.assertTrue(outputValue.contains("CDAP,Platform"));
  }

  @Ignore
  @Test
  public void testReadBlob() throws Exception {
    File testFolder = temporaryFolder.newFolder();
    File file1 = new File(testFolder, "test1");
    File file2 = new File(testFolder, "test2");
    File file3 = new File(testFolder, "empty");
    String outputDatasetName = UUID.randomUUID().toString();

    Schema schema = Schema.recordOf("blob",
                                    Schema.Field.of("body", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    String appName = UUID.randomUUID().toString();
    ApplicationManager appManager = createSourceAndDeployApp(appName, testFolder, FileFormat.BLOB.name(),
                                                             outputDatasetName, schema);

    String content1 = "abc\ndef\nghi\njkl";
    FileUtils.writeStringToFile(file1, content1);
    String content2 = "123\n456\n789";
    FileUtils.writeStringToFile(file2, content2);
    file3.createNewFile();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    byte[] byteContent1 = content1.getBytes(StandardCharsets.US_ASCII);
    byte[] byteContent2 = content2.getBytes(StandardCharsets.US_ASCII);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, output.size());

    Map<String, byte[]> contents = new HashMap<>();
    for (StructuredRecord outputRecord : output) {
      contents.put(outputRecord.get("file"), Bytes.toBytes((ByteBuffer) outputRecord.get("body")));
    }
    Assert.assertArrayEquals(byteContent1, contents.get(file1.toURI().toString()));
    Assert.assertArrayEquals(byteContent2, contents.get(file2.toURI().toString()));
  }


  @Ignore
  @Test
  public void testReadJson() throws Exception {
    File fileText = new File(temporaryFolder.newFolder(), "test.json");
    String outputDatasetName = UUID.randomUUID().toString();

    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    String appName = UUID.randomUUID().toString();
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileText, FileFormat.JSON.name(),
                                                             outputDatasetName, schema);

    StructuredRecord record1 = StructuredRecord.builder(schema).set("id", 0L).set("name", "Sam").build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("id", 1L).build();
    String fileContent = StructuredRecordStringConverter.toJsonString(record1) + "\n"
      + StructuredRecordStringConverter.toJsonString(record2);
    FileUtils.writeStringToFile(fileText, fileContent);

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    StructuredRecord expected1 = StructuredRecord.builder(schema)
      .set("id", record1.get("id"))
      .set("name", record1.get("name"))
      .set("file", fileText.toURI().toString()).build();
    StructuredRecord expected2 = StructuredRecord.builder(schema)
      .set("id", record2.get("id"))
      .set("name", record2.get("name"))
      .set("file", fileText.toURI().toString()).build();
    Set<StructuredRecord> expected = ImmutableSet.of(expected1, expected2);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testReadCSV() throws Exception {
    testReadDelimitedText(FileFormat.CSV.name(), ",");
  }

  @Ignore
  @Test
  public void testReadTSV() throws Exception {
    testReadDelimitedText(FileFormat.TSV.name(), "\t");
  }

  @Ignore
  @Test
  public void testReadDelimited() throws Exception {
    testReadDelimitedText(FileFormat.DELIMITED.name(), "\u0001");
  }

  private void testReadDelimitedText(String format, String delimiter) throws Exception {
    File fileText = new File(temporaryFolder.newFolder(), "test.txt");
    String outputDatasetName = UUID.randomUUID().toString();

    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    String appName = UUID.randomUUID().toString();
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileText, format, outputDatasetName, schema,
                                                             delimiter);

    String inputStr = new StringBuilder()
      .append("0").append("\n")
      .append("1").append(delimiter).append("\n")
      .append("2").append(delimiter).append("sam\n").toString();
    FileUtils.writeStringToFile(fileText, inputStr);

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(schema).set("id", 0L).set("file", fileText.toURI().toString()).build(),
      StructuredRecord.builder(schema).set("id", 1L).set("file", fileText.toURI().toString()).build(),
      StructuredRecord.builder(schema).set("id", 2L).set("name", "sam").set("file", fileText.toURI().toString()).build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testTextFormatWithoutOffset() throws Exception {
    File fileText = new File(temporaryFolder.newFolder(), "test.txt");
    String outputDatasetName = UUID.randomUUID().toString();

    Schema textSchema = Schema.recordOf("file.record",
                                        Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                        Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    String appName = UUID.randomUUID().toString();
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileText, "text", outputDatasetName, textSchema);

    FileUtils.writeStringToFile(fileText, "Hello,World!");

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(textSchema)
        .set("body", "Hello,World!")
        .set("file", fileText.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatText() throws Exception {
    File outputFolder = temporaryFolder.newFolder();
    File fileText = new File(outputFolder, "test.txt");
    String outputDatasetName = "test-filesource-text";

    Schema textSchema = Schema.recordOf("file.record",
                                        Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                        Schema.Field.of("file", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    String appName = "FileSourceText";
    ApplicationManager appManager = createSourceAndDeployApp(appName, outputFolder, "text",
                                                             outputDatasetName, textSchema);

    FileUtils.writeStringToFile(fileText, "Hello,World!");
    File emptyFile = new File(outputFolder, "empty");
    emptyFile.createNewFile();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(textSchema)
        .set("offset", (long) 0)
        .set("body", "Hello,World!")
        .set("file", fileText.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, output);

    // verify that the external dataset has the given schema
    verifyDatasetSchema(appName + "TestFile", textSchema);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatAvro() throws Exception {
    File fileAvro = new File(temporaryFolder.newFolder(), "test.avro");
    String outputDatasetName = "test-filesource-avro";

    String appName = "FileSourceAvro";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileAvro, "avro", outputDatasetName,
                                                             RECORD_SCHEMA);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .set("file", fileAvro.getAbsolutePath())
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, fileAvro);
    dataFileWriter.append(record);
    dataFileWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(RECORD_SCHEMA)
        .set("i", Integer.MAX_VALUE)
        .set("l", Long.MAX_VALUE)
        .set("file", fileAvro.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatAvroNullSchema() throws Exception {
    File fileAvro = new File(temporaryFolder.newFolder(), "test.avro");
    String outputDatasetName = "test-filesource-avro-null-schema";

    String appName = "FileSourceAvroNullSchema";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileAvro, "avro", outputDatasetName,
                                                             null);

    Schema recordSchemaWithoutPathField = Schema.recordOf("record",
                                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG)));

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchemaWithoutPathField.
      toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, fileAvro);
    dataFileWriter.append(record);
    dataFileWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(RECORD_SCHEMA)
        .set("i", Integer.MAX_VALUE)
        .set("l", Long.MAX_VALUE)
        .set("file", fileAvro.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatAvroMissingField() throws Exception {
    File fileAvro = new File(temporaryFolder.newFolder(), "test.avro");
    String outputDatasetName = "test-filesource-avro-missing-field";

    Schema recordSchemaWithMissingField = Schema.recordOf("record",
                                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                          Schema.Field.of("file",
                                                                          Schema.of(Schema.Type.STRING)));

    String appName = "FileSourceAvroMissingField";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileAvro, "avro", outputDatasetName,
                                                             recordSchemaWithMissingField);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .set("file", fileAvro.getAbsolutePath())
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, fileAvro);
    dataFileWriter.append(record);
    dataFileWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(recordSchemaWithMissingField)
        .set("i", Integer.MAX_VALUE)
        .set("file", fileAvro.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatParquet() throws Exception {
    File fileParquet = new File(temporaryFolder.newFolder(), "test.parquet");
    String outputDatasetName = "test-filesource-parquet";

    String appName = "FileSourceParquet";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileParquet, "parquet", outputDatasetName,
                                                             RECORD_SCHEMA);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .set("file", fileParquet.getAbsolutePath())
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");
    ParquetWriter<GenericRecord> parquetWriter = new AvroParquetWriter<>(new Path(fileParquet.getAbsolutePath()),
                                                                         avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(RECORD_SCHEMA)
        .set("i", Integer.MAX_VALUE)
        .set("l", Long.MAX_VALUE)
        .set("file", fileParquet.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);

    // verify that the external dataset has the given schema
    verifyDatasetSchema(appName + "TestFile", RECORD_SCHEMA);
  }


  @Ignore
  @Test
  public void testFileBatchInputFormatParquetNullSchema() throws Exception {
    File fileParquet = new File(temporaryFolder.newFolder(), "test.parquet");
    String outputDatasetName = "test-filesource-parquet-null-schema";

    String appName = "FileSourceParquetNullSchema";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileParquet, "parquet", outputDatasetName,
                                                             null);

    Schema recordSchemaWithMissingField = Schema.recordOf("record",
                                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG)));

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchemaWithMissingField.
      toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");
    ParquetWriter<GenericRecord> parquetWriter = new AvroParquetWriter<>(new Path(fileParquet.getAbsolutePath()),
                                                                         avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(RECORD_SCHEMA)
        .set("i", Integer.MAX_VALUE)
        .set("l", Long.MAX_VALUE)
        .set("file", fileParquet.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);
  }

  @Ignore
  @Test
  public void testFileBatchInputFormatParquetMissingField() throws Exception {
    File fileParquet = new File(temporaryFolder.newFolder(), "test.parquet");
    String outputDatasetName = "test-filesource-parquet-missing-field";

    Schema recordSchemaWithMissingField = Schema.recordOf("record",
                                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                          Schema.Field.of("file",
                                                                          Schema.of(Schema.Type.STRING)));

    String appName = "FileSourceParquetMissingField";
    ApplicationManager appManager = createSourceAndDeployApp(appName, fileParquet, "parquet", outputDatasetName,
                                                             recordSchemaWithMissingField);

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .set("file", fileParquet.getAbsolutePath())
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("TestFile");
    ParquetWriter<GenericRecord> parquetWriter = new AvroParquetWriter<>(new Path(fileParquet.getAbsolutePath()),
                                                                         avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    appManager.getWorkflowManager(SmartWorkflow.NAME)
      .startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(recordSchemaWithMissingField)
        .set("i", Integer.MAX_VALUE)
        .set("file", fileParquet.toURI().toString())
        .build()
    );

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, output);
  }

  private ApplicationManager createSourceAndDeployApp(String appName, File file, String format,
                                                      String outputDatasetName, Schema schema) throws Exception {
    return createSourceAndDeployApp(appName, file, format, outputDatasetName, schema, null);
  }

  private ApplicationManager createSourceAndDeployApp(String appName, File file, String format,
                                                      String outputDatasetName, Schema schema,
                                                      @Nullable String delimiter) throws Exception {

    ImmutableMap.Builder<String, String> sourceProperties = ImmutableMap.<String, String>builder()
      .put(Constants.Reference.REFERENCE_NAME, appName + "TestFile")
      .put(Properties.File.PATH, file.getAbsolutePath())
      .put(Properties.File.FORMAT, format)
      .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
      .put("pathField", "file");
    if (delimiter != null) {
      sourceProperties.put("delimiter", delimiter);
    }

    if (schema != null) {
      String schemaString = schema.toString();
      sourceProperties.put(Properties.File.SCHEMA, schemaString);
    }
    ETLStage source = new ETLStage(
        "source", new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties.build(), null));

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  private void verifyDatasetSchema(String dsName, Schema expectedSchema) {
    Map<String, String> metadataProperties =
      metadataAdmin.getProperties(MetadataScope.SYSTEM, MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(),
                                                                                 dsName));
    Assert.assertEquals(expectedSchema.toString(), metadataProperties.get(DatasetProperties.SCHEMA));
  }
}
