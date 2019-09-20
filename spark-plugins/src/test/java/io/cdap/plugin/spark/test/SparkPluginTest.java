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

package io.cdap.plugin.spark.test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import io.cdap.plugin.common.http.HTTPPollConfig;
import io.cdap.plugin.spark.FileStreamingSource;
import io.cdap.plugin.spark.HTTPPollerSource;
import io.cdap.plugin.spark.TwitterStreamingSource;
import io.cdap.plugin.spark.mock.MockFeedHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.HttpMethod;

/**
 * Tests for Spark plugins.
 */
public class SparkPluginTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "3.2.0");
  protected static final ArtifactSummary DATASTREAMS_ARTIFACT = new ArtifactSummary("data-streams", "3.2.0");

  private static NettyHttpService httpService;
  private static String httpBase;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();


  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for data pipeline app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      TwitterStreamingSource.class, FileStreamingSource.class,
                      HTTPPollerSource.class, HTTPPollConfig.class);

    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder("MockService")
      .setHttpHandlers(handlers)
      .build();
    httpService.start();

    int port = httpService.getBindAddress().getPort();
    httpBase = "http://localhost:" + port;
    // tell service what its port is.
    URL setPortURL = new URL(httpBase + "/port");
    HttpURLConnection urlConn = (HttpURLConnection) setPortURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write(String.valueOf(port).getBytes(Charsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    httpService.stop();
  }

  @Test
  public void testHttpStreamingSource() throws Exception {
    Assert.assertEquals(200, resetFeeds());
    final String content = "samuel jackson\ndwayne johnson\nchristopher walken";
    Assert.assertEquals(200, writeFeed("people", content));

    Map<String, String> properties = ImmutableMap.of(
      "referenceName", "peopleFeed",
      "url", httpBase + "/feeds/people",
      "interval", "1"
    );

    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", new ETLPlugin("HTTPPoller", StreamingSource.PLUGIN_TYPE, properties, null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("httpOutput")))
      .addConnection("source", "sink")
      .setBatchInterval("1s")
      .build();
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("HTTPSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    final DataSetManager<Table> outputManager = getDataset("httpOutput");
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<String> contents = new HashSet<>();
        for (StructuredRecord record : MockSink.readOutput(outputManager)) {
          contents.add(record.get("body"));
        }
        return contents.size() == 1 && contents.contains(content);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  @Test
  public void testFileSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    File folder = tmpFolder.newFolder("fileSourceTest");

    File input1 = new File(folder, "input1.txt");
    File input2 = new File(folder, "input2.csv");
    File ignore1 = new File(folder, "input1.txt.done");
    File ignore2 = new File(folder, "input1");

    CharStreams.write("1,samuel,jackson\n2,dwayne,johnson", Files.newWriterSupplier(input1, Charsets.UTF_8));
    CharStreams.write("3,christopher,walken", Files.newWriterSupplier(input2, Charsets.UTF_8));
    CharStreams.write("0,nicolas,cage", Files.newWriterSupplier(ignore1, Charsets.UTF_8));
    CharStreams.write("0,orlando,bloom", Files.newWriterSupplier(ignore2, Charsets.UTF_8));

    Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put("path", folder.getAbsolutePath())
      .put("format", "csv")
      .put("schema", schema.toString())
      .put("referenceName", "fileSourceTestInput")
      .put("ignoreThreshold", "300")
      .put("extensions", "txt,csv")
      .build();

    DataStreamsConfig pipelineCfg = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", new ETLPlugin("File", StreamingSource.PLUGIN_TYPE, properties, null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("fileOutput")))
      .addConnection("source", "sink")
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, pipelineCfg);

    ApplicationId appId = NamespaceId.DEFAULT.app("FileSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 1, TimeUnit.MINUTES);

    Map<Long, String> expected = ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken");

    final DataSetManager<Table> outputManager = getDataset("fileOutput");
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Map<Long, String> actual = new HashMap<>();
        for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
          actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
        }
        return expected.equals(actual);
      },
      4,
      TimeUnit.MINUTES);

    // now write a new file to make sure new files are picked up.
    File input3 = new File(folder, "input3.txt");

    CharStreams.write("4,terry,crews\n5,rocky,balboa", Files.newWriterSupplier(input3, Charsets.UTF_8));

    Map<Long, String> expected2 = ImmutableMap.of(4L, "terry crews", 5L, "rocky balboa");

    Table outputTable = outputManager.get();
    Scanner scanner = outputTable.scan(null, null);
    Row row;
    while ((row = scanner.next()) != null) {
      outputTable.delete(row.getRow());
    }
    outputManager.flush();

    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Map<Long, String> actual = new HashMap<>();
        for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
          actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
        }
        return expected2.equals(actual);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  private int resetFeeds() throws IOException {
    URL url = new URL(httpBase + "/feeds");
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.DELETE);
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }

  protected int writeFeed(String feedId, String content) throws IOException {
    URL url = new URL(String.format("%s/feeds/%s", httpBase, feedId));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write(content.getBytes(Charsets.UTF_8));
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }
}
