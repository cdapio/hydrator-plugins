/**
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.python.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class BatchXMLFileFilterTest extends HydratorTestBase {
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary BATCH_ARTIFACT =
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
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "1.0.1"), BATCH_APP_ARTIFACT_ID,
                      XMLReaderBatchSource.class);

    sourceFolder = temporaryFolder.newFolder("xmlSourceFolder");
    targetFolder = temporaryFolder.newFolder("xmlTargetFolder");
    sourceFolderUri = sourceFolder.toString();
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

  @Test
  public void testXMLReaderWithInvalidPatternArchiveFiles() throws Exception {
    String processedFileTable = "XMLTrackingTableWithPatternAndMoveFiles";

    System.out.println("\n\n\n" + sourceFolderUri + "\n\n\n");
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderWithPatternAndMoveFilesTest")
      .put("path", sourceFolderUri)
      .put("pattern", ".*xml$") // file ends with Large.xml
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
    try {
      startWorkflow(appManager, ProgramRunStatus.COMPLETED);
    } catch (InvalidInputException ex) {
      Assert.fail();
    }

  }

  private void startWorkflow(ApplicationManager appManager, ProgramRunStatus status) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(status, 1, 5, TimeUnit.MINUTES);
  }

  private ApplicationManager deployApplication(Map<String, String> sourceProperties, String outputDatasetName,
                                               String applicationName) throws Exception {
    ETLStage source = new ETLStage("XMLReader", new ETLPlugin("XMLReader",
                                                              BatchSource.PLUGIN_TYPE, sourceProperties,
                                                              null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId.toId(), appRequest);
  }
  
}
