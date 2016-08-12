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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.common.Constants;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.python.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link XMLReaderBatchSource} class.
 */
// TODO:(HYDRA-356) re-enable after fix
@Ignore
public class XMLReaderBatchSourceTest extends HydratorTestBase {
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());
  private static final String CATALOG_LARGE_XML_FILE_NAME = "catalogLarge.xml";
  private static final String CATALOG_SMALL_XML_FILE_NAME = "catalogSmall.xml";

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static File sourceFolder;
  private static File targetFolder;
  private static String sourceFolderUri;
  private static String targetFolderUri;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "1.0.1"), BATCH_APP_ARTIFACT_ID,
                      XMLReaderBatchSource.class);

    sourceFolder = temporaryFolder.newFolder("xmlSourceFolder");
    targetFolder = temporaryFolder.newFolder("xmlTargetFolder");
    sourceFolderUri = sourceFolder.toURI().toString();
    targetFolderUri = targetFolder.toURI().toString();
  }

  /**
   * Method to copy test xml files into source folder path, from where XMLReader read the file.
   */
  @Before
  public void copyFiles() throws IOException {
      URL largeXMLUrl = this.getClass().getResource("/" + CATALOG_LARGE_XML_FILE_NAME);
      URL smallXMLUrl = this.getClass().getResource("/" + CATALOG_SMALL_XML_FILE_NAME);
      FileUtils.copyFile(new File(largeXMLUrl.getFile()), new File(sourceFolder, CATALOG_LARGE_XML_FILE_NAME));
      FileUtils.copyFile(new File(smallXMLUrl.getFile()), new File(sourceFolder, CATALOG_SMALL_XML_FILE_NAME));
  }

  /**
   * Method to clear source and target folders. This ensures that source and target folder are ready to use for next
   * JUnit Test case.
   */
  @After
  public void clearSourceAndTargetFolder() {
    deleteFiles(sourceFolder);
    deleteFiles(targetFolder);
  }

  private void deleteFiles(File directory) {
    if (directory != null) {
      File[] files = directory.listFiles();
      if (files != null && files.length > 0) {
        for (File file : files) {
          file.delete();
        }
      }
    }
  }
  /**
   * Method to Pre-Populate File tracking KeyValue with previously processed XML file.
   */
  private void createPreProcessedRecord(String processedFileTable, Date preProcessedDate) throws Exception {
    DataSetManager<KeyValueTable> dataSetManager = getDataset(processedFileTable);
    KeyValueTable keyValueTable = dataSetManager.get();
    //Put record of processed file.
    File catalogLarge = new File(sourceFolder, CATALOG_LARGE_XML_FILE_NAME);
    keyValueTable.write(Bytes.toBytes(catalogLarge.toURI().toString()), Bytes.toBytes(preProcessedDate.getTime()));
    dataSetManager.flush();
  }

  /**
   * Method to Pre-Populate File tracking KeyValue with 40 days old expired record.
   */
  private void createExpiredRecord(String processedFileTable) throws Exception {
    DataSetManager<KeyValueTable> dataSetManager = getDataset(processedFileTable);
    KeyValueTable keyValueTable = dataSetManager.get();
    //Put expired record which is 40 days old
    File catalogSmall = new File(sourceFolder, CATALOG_SMALL_XML_FILE_NAME);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -40);
    Date expiryDate = cal.getTime();
    keyValueTable.write(Bytes.toBytes(catalogSmall.toURI().toString()), Bytes.toBytes(expiryDate.getTime()));
    dataSetManager.flush();
  }

  /**
   * Method to return currently processed XML file list.
   */
  private List<String> getProcessedFileList(String processedFileTable, Date preProcessedDate) throws Exception {
    List<String> processedFileList  = new ArrayList<String>();
    DataSetManager<KeyValueTable> dataSetManager = getDataset(processedFileTable);
    KeyValueTable table = dataSetManager.get();
    try (CloseableIterator<KeyValue<byte[], byte[]>> iterator = table.scan(null, null)) {
      if (iterator != null) {
        while (iterator.hasNext()) {
          KeyValue<byte[], byte[]> keyValue = iterator.next();
          Date date = new Date(Bytes.toLong(keyValue.getValue()));
          if (date.after(preProcessedDate)) {
            processedFileList.add(Bytes.toString(keyValue.getKey()));
          }
        }
      }
    }
    return processedFileList;
  }

  private ApplicationManager deployApplication(Map<String, String> sourceProperties, String outputDatasetName,
                                               String applicationName) throws Exception {
    ETLStage source = new ETLStage("XMLReader", new ETLPlugin("XMLReader", BatchSource.PLUGIN_TYPE, sourceProperties,
                                                              null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId.toId(), appRequest);
  }

  private void startMapReduceJob(ApplicationManager appManager) throws Exception {
    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  @Test
  /**
   * This test validate following
   * 1. Read multiple files from folder
   * 2. Delete files once processed
   * 3. Filter Pre-Processed file
   * 4. Delete 40 days old record from file tracking table.
   */
  public void testXMLReaderWithNoXMLPreProcessingRequired() throws Exception {
    String processedFileTable = "XMLTrackingTableNoXMLPreProcessingRequired";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderNoXMLPreProcessingRequiredTest")
      .put("path", sourceFolderUri)
      .put("nodePath", "/catalog/book/price")
      .put("targetFolder", targetFolderUri)
      .put("reprocessingRequired", "No")
      .put("tableName", processedFileTable)
      .put("actionAfterProcess", "Delete")
      .put("tableExpiryPeriod", "40")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-test-no-preprocessing-required";
    ApplicationManager appManager = deployApplication(sourceProperties, outputDatasetName,
                                                      "XMLReaderNoXMLPreProcessingRequiredTest");

    Date preProcessedDate = new Date();
    createPreProcessedRecord(processedFileTable, preProcessedDate);
    createExpiredRecord(processedFileTable);

    startMapReduceJob(appManager);

    //Nmber of files processed
    List<String> processedFileList = getProcessedFileList(processedFileTable, preProcessedDate);
    Assert.assertEquals(1, processedFileList.size());

    //Number of record derived.
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(3, output.size());
    //Source folder left with one pre-processed file
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(1, sourceFiles.length);
  }

  @Test
  /**
   * This test validate following
   * 1. Read multiple files from folder
   * 2. PreProcessing of files
   */
  public void testXMLReaderWithXMLPreProcessingRequired() throws Exception {
    String processedFileTable = "XMLTrackingTableXMLPreProcessingRequired";

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderXMLPreProcessingRequiredTest")
      .put("path", sourceFolderUri)
      .put("nodePath", "/catalog/book/price")
      .put("targetFolder", targetFolderUri)
      .put("reprocessingRequired", "Yes")
      .put("tableName", processedFileTable)
      .put("actionAfterProcess", "None")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-test-preprocessing-required";
    ApplicationManager appManager = deployApplication(sourceProperties, outputDatasetName,
                                                      "XMLReaderXMLPreProcessingRequiredTest");

    Date preProcessedDate = new Date();
    createPreProcessedRecord(processedFileTable, preProcessedDate);

    startMapReduceJob(appManager);

    List<String> processedFileList = getProcessedFileList(processedFileTable, preProcessedDate);
    Assert.assertEquals(2, processedFileList.size());

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(12, output.size());
  }

  @Test
  public void testXMLReaderWithInvalidNodePathArchiveFiles() throws Exception {
    String processedFileTable = "XMLTrackingTableInvalidNodePathArchiveFiles";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderInvalidNodePathArchiveFilesTest")
      .put("path", sourceFolderUri)
      .put("nodePath", "/catalog/book/prices")
      .put("targetFolder", targetFolderUri)
      .put("reprocessingRequired", "No")
      .put("tableName", processedFileTable)
      .put("actionAfterProcess", "archive")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-test-invalid-node-path-archived-files";
    ApplicationManager appManager = deployApplication(sourceProperties, outputDatasetName,
                                                      "XMLReaderInvalidNodePathArchiveFilesTest");
    startMapReduceJob(appManager);

    //No records for invalid node path
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(0, output.size());

    //Source folder must have 0 files after archive
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(0, sourceFiles.length);

    //Target folder must have 2 archived zip and 2 corresponding .crc files
    File[] targetFiles = targetFolder.listFiles();
    Assert.assertEquals(4, targetFiles.length);
  }

  @Test
  public void testXMLReaderWithPatternAndMoveFiles() throws Exception {
    String processedFileTable = "XMLTrackingTableWithPatternAndMoveFiles";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderWithPatternAndMoveFilesTest")
      .put("path", sourceFolderUri)
      .put("pattern", "Large.xml$") // file ends with Large.xml
      .put("nodePath", "/catalog/book/price")
      .put("targetFolder", targetFolderUri)
      .put("reprocessingRequired", "No")
      .put("tableName", processedFileTable)
      .put("actionAfterProcess", "move")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-test-pattern-move-files";
    ApplicationManager appManager = deployApplication(sourceProperties, outputDatasetName,
                                                      "XMLReaderWithPatternAndMoveFilesTest");
    startMapReduceJob(appManager);

    //Number of record derived from XML.
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(9, output.size());

    //Source folder must have 1 unprocessed file
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(1, sourceFiles.length);

    //Sarget folder must have 1 moved file
    File[] targetFiles = targetFolder.listFiles();
    Assert.assertEquals(1, targetFiles.length);
  }

  @Test
  public void testXMLReaderWithInvalidPatternArchiveFiles() throws Exception {
    String processedFileTable = "XMLTrackingTableInvalidPatternDeleteFiles";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderInvalidPatternDeleteFilesTest")
      .put("path", sourceFolderUri)
      .put("pattern", "^catalogMedium") //file name start with catalogMedium, does not exist
      .put("nodePath", "/catalog/book/price")
      .put("targetFolder", targetFolderUri)
      .put("reprocessingRequired", "No")
      .put("tableName", processedFileTable)
      .put("actionAfterProcess", "archive")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-test-invalid-pattern-delete-files";
    ApplicationManager appManager = deployApplication(sourceProperties, outputDatasetName,
                                                      "XMLReaderInvalidPatternDeleteFilesTest");
    startMapReduceJob(appManager);

    //No record fetched as no pattern matching file
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    Assert.assertEquals(0, output.size());

    //Source folder must have 2 files, no file archived and deleted
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(2, sourceFiles.length);
  }
}
