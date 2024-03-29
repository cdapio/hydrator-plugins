/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link ExcelInputReader} class.
 */
@Ignore
public class ExcelInputReaderTest extends HydratorTestBase {

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary BATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static File sourceFolder;
  private static String sourceFolderUri;
  private static String excelTestFileOne = "/civil_test_data_one.xlsx";
  private static String excelTestFileTwo = "/civil_test_data_two.xlsx";


  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("excelreader-plugins", "4.0.0"), BATCH_APP_ARTIFACT_ID,
                      ExcelInputReader.class);

    sourceFolder = temporaryFolder.newFolder("ExcelInputReaderFolder");
    sourceFolderUri = sourceFolder.toURI().toString();
  }

  @Before
  public void copyFiles() throws Exception {
    URL testFileUrl = this.getClass().getResource(excelTestFileOne);
    URL testTwofileUrl = this.getClass().getResource(excelTestFileTwo);

    FileUtils.copyFile(new File(testFileUrl.getFile()), new File(sourceFolder, excelTestFileOne));
    FileUtils.copyFile(new File(testTwofileUrl.getFile()), new File(sourceFolder, excelTestFileTwo));
  }

  @Test
  public void testExcelWithDate() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, "TestCase-testExcel")
        .put("filePath", sourceFolderUri)
        .put("filePattern", ".*")
        .put("sheet", "Sheet Name")
        .put("sheetValue", "Sheet1")
        .put("memoryTableName", "trackMemoryTable")
        .put("tableExpiryPeriod", "30")
        .put("reprocess", "false")
        .put("columnList", "")
        .put("columnMapping", "")
        .put("skipFirstRow", "false")
        .put("terminateIfEmptyRow", "false")
        .put("rowsLimit", "")
        .put("outputSchema", "A:string,B:string,C:String,D:String,E:String,F:String")
        .put("ifErrorRecord", "Ignore error and continue")
        .put("errorDatasetName", "")
        .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
        sourceProperties, null));

    String outputDatasetName = "output-testExcel";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "ExcelTests");
    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    Date dateRow1 = dateFormat.parse((String) output.get(1).get("F"));
    Calendar cal = Calendar.getInstance();
    cal.setTime(dateRow1);
    Assert.assertEquals(2018, cal.get(Calendar.YEAR));
    Assert.assertEquals(1, cal.get(Calendar.MONTH) + 1);
    Assert.assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
  }
  @Test
  public void testExcelInputReader() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase-testExcel")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTable")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "A:string,B:string,C:String,D:String,E:String,F:String")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testExcel";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "ExcelTests");
    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Map<String, String> nameIdMap = new HashMap<String, String>();
    nameIdMap.put("john", "3.0");
    nameIdMap.put("romy", "1.0");
    nameIdMap.put("Paulo", "11.0");
    nameIdMap.put("Ruskin", "10.0");
    nameIdMap.put("Alan", "8.0");
    nameIdMap.put("Bill", "13.0");
    nameIdMap.put("Ada", "14.0");
    nameIdMap.put("kelly", "9.0");
    nameIdMap.put("name", "id");

    Assert.assertEquals(nameIdMap.get(output.get(0).get("B")), output.get(0).get("A"));
    Assert.assertEquals(nameIdMap.get(output.get(1).get("B")), output.get(1).get("A"));
    Assert.assertEquals(nameIdMap.get(output.get(2).get("B")), output.get(2).get("A"));
    Assert.assertEquals(nameIdMap.get(output.get(3).get("B")), output.get(3).get("A"));

    Assert.assertEquals("Expected records", 9, output.size());
  }

  @Test
  public void testWithReProcessTrue() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Number")
      .put("sheetValue", "0")
      .put("memoryTableName", "trackMemoryTableWithReProcessedTrue")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "true")
      .put("columnList", "")
      .put("columnMapping", "A:FirstColumn")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "10")
      .put("outputSchema", "A:string,B:string,C:String,D:String,E:String,F:String")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithReProcessedTrue");

    DataSetManager<KeyValueTable> dataSetManager = getDataset("trackMemoryTableWithReProcessedTrue");
    KeyValueTable keyValueTable = dataSetManager.get();

    File testFile = new File(sourceFolder, excelTestFileTwo);
    keyValueTable.write(testFile.toURI().toString(), String.valueOf(System.currentTimeMillis()));
    dataSetManager.flush();

    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 9, output.size());
    Assert.assertNotNull(output.get(1).getSchema().getField("FirstColumn"));
  }

  @Test
  public void testWithReProcessedFalse() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithReProcessedFalse")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "10")
      .put("outputSchema", "A:string,B:string,C:String,D:String,E:String,F:String")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithReProcessedFalse";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithReProcessedFalse");

    DataSetManager<KeyValueTable> dataSetManager = getDataset("trackMemoryTableWithReProcessedFalse");
    KeyValueTable keyValueTable = dataSetManager.get();

    File testFile = new File(sourceFolder, excelTestFileTwo);
    keyValueTable.write(testFile.toURI().toString(), String.valueOf(System.currentTimeMillis()));

    dataSetManager.flush();

    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Map<String, String> nameIdMap = new HashMap<>();
    nameIdMap.put("john", "3.0");
    nameIdMap.put("romy", "1.0");
    nameIdMap.put("name", "id");

    Assert.assertEquals("Expected records", 3, output.size());

    Assert.assertEquals(nameIdMap.get(output.get(0).get("B")), output.get(0).get("A"));
    Assert.assertEquals(nameIdMap.get(output.get(1).get("B")), output.get(1).get("A"));
    Assert.assertEquals(nameIdMap.get(output.get(2).get("B")), output.get(2).get("A"));
  }

  @Test
  public void testWithColumnsToBeExtracted() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithColumnsToBeExtracted")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "A,B")
      .put("columnMapping", "A:FirstColumn")
      .put("skipFirstRow", "true")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "2")
      .put("outputSchema", "")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithColumnsToBeExtracted";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithColumnsToBeExtracted");
    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 4, output.size());

    Assert.assertNotNull(output.get(1).getSchema().getField("FirstColumn"));
    Assert.assertNotNull(output.get(1).getSchema().getFields().contains("B"));

    Map<String, String> nameIdMap = new HashMap<>();
    nameIdMap.put("john", "3.0");
    nameIdMap.put("romy", "1.0");
    nameIdMap.put("Paulo", "11.0");
    nameIdMap.put("Ruskin", "10.0");
    nameIdMap.put("Bill", "13.0");
    nameIdMap.put("Ada", "14.0");
    nameIdMap.put("kelly", "9.0");

    Assert.assertEquals(nameIdMap.get(output.get(0).get("B")), output.get(0).get("FirstColumn"));
    Assert.assertEquals(nameIdMap.get(output.get(1).get("B")), output.get(1).get("FirstColumn"));
    Assert.assertEquals(nameIdMap.get(output.get(2).get("B")), output.get(2).get("FirstColumn"));
  }

  @Test
  public void testWithErrorRecord() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithErrorRecord")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "A,B,C")
      .put("columnMapping", "A:FirstColumn")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "A:double,C:double")
      .put("ifErrorRecord", "Write to error dataset")
      .put("errorDatasetName", "error-dataset-table")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithErrorRecord";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithErrorRecord");
    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 6, output.size());

    DataSetManager<Table> errorTableManager = getDataset("error-dataset-table");
    Table errorTable = errorTableManager.get();
    Scanner scanner = errorTable.scan(null, null);
    int counter = 0;
    while (scanner.next() != null) {
      counter++;
    }

    Assert.assertEquals("Expected error records", 3, counter);
  }


  @Test(expected = IllegalStateException.class)
  public void testWithNoColumnListAndNoOutputSchema() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithNoColumnListAndOutputSchema")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithNoColumnListAndOutputSchema";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    deployApp(source, sink, "testWithNoColumnListAndOutputSchema");
    Assert.fail();
  }

  @Test
  public void testWithTerminateIfEmptyRow() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Number")
      .put("sheetValue", "0")
      .put("memoryTableName", "trackMemoryTableWithTerminateIfEmptyRow")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "true")
      .put("rowsLimit", "")
      .put("outputSchema", "A:string,B:string,C:string,D:string,Romy:string")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithTerminateIfEmptyRow";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithTerminateIfEmptyRow");
    startWorkflow(appManager, ProgramRunStatus.FAILED);
  }

  @Test(expected = IllegalStateException.class)
  public void testWithNoErrorDataset() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithSkipFirstRow")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "A")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "")
      .put("ifErrorRecord", "Write to error dataset")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithSkipFirstRow";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    deployApp(source, sink, "testWithSkipFirstRow");
    Assert.fail();
  }

  @Test(expected = IllegalStateException.class)
  public void testWithInvalidSheetNumber() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Number")
      .put("sheetValue", "-1")
      .put("memoryTableName", "trackMemoryTableWithNoColumnListAndOutputSchema")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithNoColumnListAndOutputSchema";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    deployApp(source, sink, "testWithNoColumnListAndOutputSchema");
    Assert.fail();
  }

  @Test
  public void testWithTTL() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Number")
      .put("sheetValue", "0")
      .put("memoryTableName", "trackMemoryTableWithTTL")
      .put("tableExpiryPeriod", "15")
      .put("reprocess", "false")
      .put("columnMapping", "A:FirstColumn")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "10")
      .put("outputSchema", "A:string")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-WithTTL";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ApplicationManager appManager = deployApp(source, sink, "testWithTTL");

    DataSetManager<KeyValueTable> dataSetManager = getDataset("trackMemoryTableWithTTL");
    KeyValueTable keyValueTable = dataSetManager.get();

    File testFile = new File(sourceFolder, excelTestFileTwo);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -20);
    keyValueTable.write(Bytes.toBytes(testFile.toURI().toString()), Bytes.toBytes(cal.getTimeInMillis()));
    dataSetManager.flush();

    startWorkflow(appManager, ProgramRunStatus.COMPLETED);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 9, output.size());
    Assert.assertNotNull(output.get(1).getSchema().getField("FirstColumn"));
  }

  private ApplicationManager deployApp(ETLStage source, ETLStage sink, String appName) throws Exception {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  private WorkflowManager startWorkflow(ApplicationManager appManager, ProgramRunStatus status) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(status, 1, 5, TimeUnit.MINUTES);
    return workflowManager;
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidRowLimit() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestInValidRowLimit")
      .put("filePath", sourceFolderUri)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTableWithInValidRowLimit")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "A")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "no")
      .put("outputSchema", "")
      .put("ifErrorRecord", "Write to error dataset")
      .put("errorDatasetName", "")
      .build();

    ETLStage source = new ETLStage("ExcelInputtest", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-testWithInValidRowLimit";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    deployApp(source, sink, "testWithInValidRowLimit");
    Assert.fail();
  }
}
