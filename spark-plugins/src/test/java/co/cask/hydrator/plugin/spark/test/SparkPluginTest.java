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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datastreams.DataStreamsApp;
import co.cask.cdap.datastreams.DataStreamsSparkLauncher;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
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
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.common.http.HTTPPollConfig;
import co.cask.hydrator.plugin.spark.HTTPPollerSource;
import co.cask.hydrator.plugin.spark.KafkaStreamingSource;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import co.cask.hydrator.plugin.spark.mock.MockFeedHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import kafka.common.TopicAndPartition;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
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

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;
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
      new ArtifactRange(NamespaceId.DEFAULT, DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT, DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      KafkaStreamingSource.class, KafkaUtils.class, DefaultDecoder.class, TopicAndPartition.class,
                      TwitterStreamingSource.class,
                      HTTPPollerSource.class, HTTPPollConfig.class);

    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(),
                                                              kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();


    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder("MockService")
      .addHttpHandlers(handlers)
      .build();
    httpService.startAndWait();

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
  public static void cleanup() {
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
    httpService.stopAndWait();
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
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Set<String> contents = new HashSet<>();
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            contents.add((String) record.get("body"));
          }
          return contents.size() == 1 && contents.contains(content);
        }
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
    sparkManager.waitForStatus(true, 10, 1);

    final Map<Long, String> expected = ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken"
    );

    final DataSetManager<Table> outputManager = getDataset("fileOutput");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> actual = new HashMap<>();
          for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
            actual.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
          }
          return expected.equals(actual);
        }
      },
      4,
      TimeUnit.MINUTES);

    // now write a new file to make sure new files are picked up.
    File input3 = new File(folder, "input3.txt");

    CharStreams.write("4,terry,crews\n5,rocky,balboa", Files.newWriterSupplier(input3, Charsets.UTF_8));

    final Map<Long, String> expected2 = ImmutableMap.of(
      4L, "terry crews",
      5L, "rocky balboa"
    );

    Table outputTable = outputManager.get();
    Scanner scanner = outputTable.scan(null, null);
    Row row;
    while ((row = scanner.next()) != null) {
      outputTable.delete(row.getRow());
    }
    outputManager.flush();

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> actual = new HashMap<>();
          for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
            actual.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
          }
          return expected2.equals(actual);
        }
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  @Test
  public void testKafkaStreamingSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", "kafkaPurchases");
    properties.put("brokers", "localhost:" + kafkaPort);
    properties.put("topic", "users");
    properties.put("defaultInitialOffset", "-2");
    properties.put("format", "csv");
    properties.put("schema", schema.toString());

    ETLStage source = new ETLStage("source", new ETLPlugin("Kafka", StreamingSource.PLUGIN_TYPE, properties, null));

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(new ETLStage("sink", MockSink.getPlugin("kafkaOutput")))
      .addConnection("source", "sink")
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KafkaSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write some messages to kafka
    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "3,christopher,walken");
    sendKafkaMessage("users", messages);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    final Map<Long, String> expected = ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken"
    );

    final DataSetManager<Table> outputManager = getDataset("kafkaOutput");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> actual = new HashMap<>();
          for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
            actual.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
          }
          return expected.equals(actual);
        }
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);

    // clear the output table
    Table outputTable = outputManager.get();
    Scanner scanner = outputTable.scan(null, null);
    Row row;
    while ((row = scanner.next()) != null) {
      outputTable.delete(row.getRow());
    }
    outputManager.flush();

    // now write some more messages to kafka and start the program again to make sure it picks up where it left off
    messages = new HashMap<>();
    messages.put("d", "4,terry,crews");
    messages.put("e", "5,sylvester,stallone");
    sendKafkaMessage("users", messages);

    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    final Map<Long, String> expected2 = ImmutableMap.of(
      4L, "terry crews",
      5L, "sylvester stallone"
    );
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> actual = new HashMap<>();
          for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
            actual.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
          }
          return expected2.equals(actual);
        }
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }

  private void sendKafkaMessage(String topic, Map<String, String> messages) {
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);

    // If publish failed, retry up to 20 times, with 100ms delay between each retry
    // This is because leader election in Kafka 08 takes time when a topic is being created upon publish request.
    int count = 0;
    do {
      KafkaPublisher.Preparer preparer = publisher.prepare(topic);
      for (Map.Entry<String, String> entry : messages.entrySet()) {
        preparer.add(Charsets.UTF_8.encode(entry.getValue()), entry.getKey());
      }
      try {
        preparer.send().get();
        break;
      } catch (Exception e) {
        // Backoff if send failed.
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    } while (count++ < 20);
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
