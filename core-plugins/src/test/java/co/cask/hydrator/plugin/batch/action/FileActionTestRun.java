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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
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
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link FileAction}
 */
public class FileActionTestRun extends HydratorTestBase {
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.4.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());
  private static final String CATALOG_LARGE_XML_FILE_NAME = "catalogLarge.xml";
  private static final String CATALOG_SMALL_XML_FILE_NAME = "catalogSmall.xml";

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
                      FileAction.class);

    sourceFolder = temporaryFolder.newFolder("xmlSourceFolder");
    targetFolder = temporaryFolder.newFolder("xmlTargetFolder");
    sourceFolderUri = sourceFolder.toURI().toString();
    targetFolderUri = targetFolder.toURI().toString();
  }

  /**
   * Method to copy test xml files into source folder, which will be used by the test cases.
   */
  @Before
  public void copyFiles() throws IOException {
    URL largeXMLUrl = this.getClass().getResource("/" + CATALOG_LARGE_XML_FILE_NAME);
    URL smallXMLUrl = this.getClass().getResource("/" + CATALOG_SMALL_XML_FILE_NAME);
    FileUtils.copyFile(new File(largeXMLUrl.getFile()), new File(sourceFolder, CATALOG_LARGE_XML_FILE_NAME));
    FileUtils.copyFile(new File(smallXMLUrl.getFile()), new File(sourceFolder, CATALOG_SMALL_XML_FILE_NAME));
  }

  /**
   * Method to clear source and target folders.
   * This ensures that source and target folder are ready to use for next Test case.
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

  private ApplicationManager deployApplication(String fileAction, String applicationName, String sourceDataset,
                                               String sinkDataset) throws Exception {
    ETLStage action = new ETLStage("fileAction", new ETLPlugin("FileAction", PostAction.PLUGIN_TYPE,
                                                               ImmutableMap.of("path", sourceFolderUri,
                                                                               "targetFolder", targetFolderUri,
                                                                               "action", fileAction,
                                                                               "runCondition", "completion"), null));

    ETLStage source = new ETLStage("source", MockSource.getPlugin(sourceDataset));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkDataset));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(action)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId.toId(), appRequest);
  }

  private void startMapReduceJob(ApplicationManager appManager) throws Exception {
    WorkflowManager manager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    manager.start(ImmutableMap.of("logical.start.time", "0"));
    manager.waitForFinish(5, TimeUnit.MINUTES);
  }

  @Test
  public void testFileActionNone() throws Exception {
    ApplicationManager appManager = deployApplication("None", "fileActionNoneTest", "fileActionNoneSource",
                                                      "fileActionNoneSink");
    startMapReduceJob(appManager);
    //Source folder left with 2 files
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(2, sourceFiles.length);
  }

  @Test
  public void testFileActionDelete() throws Exception {
    ApplicationManager appManager = deployApplication(FileAction.DELETE_ACTION, "fileActionDeleteTest",
                                                      "fileActionDeleteSource", "fileActionDeleteSink");
    startMapReduceJob(appManager);
    //Source folder left with 0 files after delete
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(0, sourceFiles.length);
  }

  @Test
  public void testFileActionMove() throws Exception {
    ApplicationManager appManager = deployApplication(FileAction.MOVE_ACTION, "fileActionMoveTest",
                                                      "fileActionMoveSource", "fileActionMoveSink");
    startMapReduceJob(appManager);
    //Source folder left with 0 files after move
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(0, sourceFiles.length);
    //Target folder must have 2 moved file
    File[] targetFiles = targetFolder.listFiles();
    Assert.assertEquals(2, targetFiles.length);
  }

  @Test
  public void testFileActionArchive() throws Exception {
    ApplicationManager appManager = deployApplication(FileAction.ARCHIVE_ACTION, "fileActionArchiveTest",
                                                      "fileActionArchiveSource", "fileActionArchiveSink");
    startMapReduceJob(appManager);
    //Source folder left with 0 files after move
    File[] sourceFiles = sourceFolder.listFiles();
    Assert.assertEquals(0, sourceFiles.length);
    //Target folder must have 2 zip and 2 crc file
    File[] targetFiles = targetFolder.listFiles();
    Assert.assertEquals(4, targetFiles.length);
  }

  @Test
  public void testValidConfig() throws Exception {
    String path = "/opt/hdfs/catalog.xml";
    String targetFolder = "/opt/hdfs/target";
    FileAction.Config config = new FileAction.Config(path, targetFolder, FileAction.DELETE_ACTION, null);
    Assert.assertEquals(path, config.getPath());
    Assert.assertEquals(targetFolder, config.getTargetFolder());
    Assert.assertEquals(FileAction.DELETE_ACTION, config.getAction());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyPathConfig() throws Exception {
    FileAction.Config config = new FileAction.Config(null, "/opt/hdfs/target", FileAction.MOVE_ACTION, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmtyTargetForMoveConfig() throws Exception {
    FileAction.Config config = new FileAction.Config("/opt/hdfs/catalog.xml", null, FileAction.MOVE_ACTION, null);
    config.validate();
  }
}
