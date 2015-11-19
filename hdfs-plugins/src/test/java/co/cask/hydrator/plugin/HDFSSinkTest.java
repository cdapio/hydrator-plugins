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

package co.cask.hydrator.plugin;

import co.cask.StreamBatchSource;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link HDFSSink}.
 */
public class HDFSSinkTest extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSSinkTest.class);

  private static final String STREAM_NAME = "myStream";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final Id.Artifact BATCH_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                            "etlbatch", CURRENT_VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT = ArtifactSummary.from(BATCH_APP_ARTIFACT_ID);

  private MiniDFSCluster dfsCluster;
  private FileSystem fileSystem;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add artifact for batch sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      HDFSSink.class, StreamBatchSource.class);

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
    fileSystem = FileSystem.get(conf);
  }

  @After
  public void afterTest() throws Exception {
    // Shutdown Hadoop Minicluster
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Test
  public void testHDFSSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send("AAPL|10|400.23");
    streamManager.send("CDAP|13|123.23");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, STREAM_NAME)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    Path outputDir = dfsCluster.getFileSystem().getHomeDirectory();
    ETLStage sink = new ETLStage("HDFS", ImmutableMap.<String, String>builder()
      .put("path", outputDir.toUri().toString())
      .build());
    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "HDFSTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Path[] outputFiles = FileUtil.stat2Paths(dfsCluster.getFileSystem().listStatus(
      outputDir, new Utils.OutputFileUtils.OutputFilesFilter()));
    Assert.assertNotNull(outputFiles);
    Assert.assertTrue(outputFiles.length > 0);
    int count = 0;
    List<String> lines = new ArrayList<>();
    for (Path path : outputFiles) {
      InputStream in = dfsCluster.getFileSystem().open(path);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
        if (line.contains("AAPL") || line.contains("CDAP")) {
          count++;
        }
      }
      reader.close();
    }
    Assert.assertEquals(2, lines.size());
    Assert.assertEquals(2, count);
  }
}
