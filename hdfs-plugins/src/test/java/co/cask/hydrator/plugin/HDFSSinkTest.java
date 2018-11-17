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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
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
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link HDFSSink}.
 */
public class HDFSSinkTest extends HydratorTestBase {

  private static final Schema SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("hdfs-plugins", "1.0.0"),
                      BATCH_APP_ARTIFACT_ID,
                      HDFSSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Setup Hadoop Minicluster
    File baseDir = temporaryFolder.newFolder();
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    dfsCluster = builder.build();
    dfsCluster.waitActive();
  }

  @After
  public void afterTest() throws Exception {
    // Shutdown Hadoop Minicluster
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Ignore
  @Test
  public void testHDFSSink() throws Exception {
    String inputDatasetName = "input-hdfssinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Path outputDir = dfsCluster.getFileSystem().getHomeDirectory();
    ETLStage sink = new ETLStage("HDFS", new ETLPlugin(
      "HDFS",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("path", outputDir.toUri().toString())
        .put(Constants.Reference.REFERENCE_NAME, "HDFSinkTest")
        .put("delimiter", "|")
        .build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("HDFSTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SCHEMA).set("ticker", null).set("num", 10).set("price", 400.23).build(),
      StructuredRecord.builder(SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 123.23).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    Path[] outputFiles = FileUtil.stat2Paths(dfsCluster.getFileSystem().listStatus(
      outputDir, new Utils.OutputFileUtils.OutputFilesFilter()));
    Assert.assertNotNull(outputFiles);
    Assert.assertTrue(outputFiles.length > 0);
    Set<String> lines = new HashSet<>();
    for (Path path : outputFiles) {
      InputStream in = dfsCluster.getFileSystem().open(path);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
      reader.close();
    }
    Assert.assertEquals(ImmutableSet.of("\0|10|400.23", "CDAP|13|123.23"), lines);
  }

  @Ignore
  @Test
  public void testAddingJobProperties() throws Exception {
    String inputDatasetName = "input-hdfssinktest";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Path outputDir = dfsCluster.getFileSystem().getHomeDirectory();
    ETLStage sink = new ETLStage("HDFS", new ETLPlugin(
      "HDFS",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("path", outputDir.toUri().toString())
        .put(Constants.Reference.REFERENCE_NAME, "HDFSinkTest")
        .put("jobProperties", "{" +
          "\"mapreduce.output.fileoutputformat.compress\":\"true\"," +
          "\"mapreduce.output.fileoutputformat.compress.codec\":\"org.apache.hadoop.io.compress.DefaultCodec\"," +
          "\"mapreduce.output.fileoutputformat.compress.type\":\"BLOCK\"" +
          "}")
        .build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("HDFSTest-adding-jobproperties");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SCHEMA).set("ticker", null).set("num", 10).set("price", 400.23).build(),
      StructuredRecord.builder(SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 123.23).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    Path[] outputFiles = FileUtil.stat2Paths(dfsCluster.getFileSystem().listStatus(
      outputDir, new Utils.OutputFileUtils.OutputFilesFilter()));
    Assert.assertNotNull(outputFiles);
    Assert.assertTrue(outputFiles.length > 0);
    for (Path path : outputFiles) {
      Assert.assertTrue(path.getName().endsWith(".deflate"));
    }
  }
}
