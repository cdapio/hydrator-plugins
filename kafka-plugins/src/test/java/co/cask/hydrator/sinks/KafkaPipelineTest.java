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

package co.cask.hydrator.sinks;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.plugin.realtime.KafkaProducer;
import co.cask.hydrator.plugin.realtime.KafkaSource;
import co.cask.hydrator.sinks.test.RealtimeTableSink;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
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
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 */
public class KafkaPipelineTest extends TestBase {
  private static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "etlrealtime", "3.2.0");
  private static final ArtifactSummary APP_ARTIFACT = ArtifactSummary.from(APP_ARTIFACT_ID);
  private static final int PARTITIONS = 1;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, true);

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void setupTests() throws Exception {
    //add the artifact for the etl realtime app
    addAppArtifact(APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add artifact for plugins
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "kafka-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      KafkaSource.class, KafkaProducer.class);

    // add artifact for test
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      RealtimeTableSink.class);

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
  }

  @AfterClass
  public static void cleanup() {
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testKafkaSource() throws Exception {
    Schema schema = Schema.recordOf("student",
                                    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("AGE", Schema.of(Schema.Type.INT)));
    Plugin source = new Plugin("Kafka", ImmutableMap.<String, String>builder()
      .put(KafkaSource.KAFKA_TOPIC, "MyTopic")
      .put(KafkaSource.KAFKA_ZOOKEEPER, zkServer.getConnectionStr())
      .put(KafkaSource.FORMAT, "csv")
      .put(KafkaSource.SCHEMA, schema.toString())
      .put(KafkaSource.KAFKA_PARTITIONS, Integer.toString(PARTITIONS))
      .build()
    );

    Plugin sink = new Plugin("Table", ImmutableMap.of(
      "name", "outputTable",
      "schema", schema.toString(),
      "schema.row.field", "NAME"));

    Map<String, String> message = Maps.newHashMap();
    message.put("1", "Bob,1,3");
    sendMessage("MyTopic", message);

    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(new ETLStage("source", source), new ETLStage("sink", sink));

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    workerManager.start();
    DataSetManager<Table> tableManager = getDataset("outputTable");
    waitForTableToBePopulated(tableManager);
    workerManager.stop();

    // verify. We need to get the table fresh because it doesn't update otherwise
    Table table = tableManager.get();
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals(1, (int) row.getInt("ID"));
    Assert.assertEquals(3, (int) row.getInt("AGE"));

    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("select NAME,ID,AGE from dataset_outputTable").executeQuery();
    Assert.assertTrue(results.next());
    Assert.assertEquals("Bob", results.getString(1));
    Assert.assertEquals(1, results.getInt(2));
    Assert.assertEquals(3, results.getInt(3));
  }

  private void waitForTableToBePopulated(final DataSetManager<Table> tableManager) throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Table table = tableManager.get();
        Row row = table.get("Bob".getBytes(Charsets.UTF_8));
        // need to wait for information to get to the table, not just for the row to be created
        return row.getColumns().size() != 0;
      }
    }, 10, TimeUnit.SECONDS);
  }

  private static java.util.Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    // Note: the log size properties below have been set so that we can have log rollovers
    // and log deletions in a minute.
    java.util.Properties prop = new java.util.Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", Integer.toString(PARTITIONS));
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "100");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("log.retention.bytes", "1000");
    prop.setProperty("log.retention.check.interval.ms", "60000");

    return prop;
  }

  protected void sendMessage(String topic, Map<String, String> messages) {
    // Publish a message to Kafka, the flow should consume it
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
}
