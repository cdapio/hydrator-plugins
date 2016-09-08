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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.mock.realtime.MockRealtimeContext;
import co.cask.hydrator.plugin.realtime.KafkaProducer;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Charsets;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Producer Test Cases. 
 */
public class KafkaProducerTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerTest.class);

  // In Memory Zookeeper and Kafka Server.
  protected static InMemoryZKServer zkServer;
  protected static EmbeddedKafkaServer kafkaServer;

  // Local In Memory Zookeeper and Kafka Client. 
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  // Number of partitions configured.
  protected static final int PARTITIONS = 4;

  // Port on which Kafka broker would be running. 
  protected static int kafkaPort;

  // Input Schema that would be injested into Kafka.
  private static final Schema INPUT = Schema.recordOf("input",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)));

  /**
   * Tests KafkaProducer for publishing structured records as JSON.
   * This also tests the partitioning by pushing it to 4 partitions
   * derived from the field 'c'. It then collects the messages as per 
   * the partition position.
   *
   * @throws Exception in case of any problems. 
   */
  @Test
  public void testJSONPublish() throws Exception {
    String testTopic = "json";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "TRUE", "c", "b", testTopic, "JSON");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());
    
    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first").set("c", 1).set("d", 12.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth").set("c", 4).set("d", 15.34)
                .set("e", true).build());
    kafkaproducer.write(input, null);
    
    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
      kafkaClient.getConsumer().prepare()
        .addFromBeginning(testTopic, 0)
        .addFromBeginning(testTopic, 1)
        .addFromBeginning(testTopic, 2)
        .addFromBeginning(testTopic, 3)
        .consume(new KafkaConsumer.MessageCallback() {
          @Override
          public void onReceived(Iterator<FetchedMessage> messages) {
            while (messages.hasNext()) {
              FetchedMessage msg = messages.next();
              // Add to array with partition id as index.
              consumedMessages.add(msg.getTopicPartition().getPartition(), 
                                   Charsets.UTF_8.decode(msg.getPayload()).toString());
              latch.countDown();
            }
          }

          @Override
          public void finished() {
          }
        });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("{\"a\":4,\"b\":\"fourth\",\"c\":4,\"d\":15.34,\"e\":true}", consumedMessages.get(0));
    Assert.assertEquals("{\"a\":1,\"b\":\"first\",\"c\":1,\"d\":12.34,\"e\":false}", consumedMessages.get(1));
    Assert.assertEquals("{\"a\":2,\"b\":\"second\",\"c\":2,\"d\":13.34,\"e\":true}", consumedMessages.get(2));
    Assert.assertEquals("{\"a\":3,\"b\":\"third\",\"c\":3,\"d\":14.34,\"e\":false}", consumedMessages.get(3));
    kafkaproducer.destroy();
  }

  @Test
  public void testCSVPublish() throws Exception {
    String testTopic = "csv";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "FALSE", "c", "b", testTopic, "CSV");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());

    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first 1").set("c", 1).set("d", 12.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second 2").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third 3").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth 4").set("c", 4).set("d", 15.342423442424)
                .set("e", true).build());
    kafkaproducer.write(input, null);

    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
    kafkaClient.getConsumer().prepare()
      .addFromBeginning(testTopic, 0)
      .addFromBeginning(testTopic, 1)
      .addFromBeginning(testTopic, 2)
      .addFromBeginning(testTopic, 3)
      .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage msg = messages.next();
            // Add to array with partition id as index.
            consumedMessages.add(msg.getTopicPartition().getPartition(),
                                 Charsets.UTF_8.decode(msg.getPayload()).toString());
            latch.countDown();
          }
        }

        @Override
        public void finished() {
        }
      });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("4,fourth 4,4,15.342423442424,true\r\n", consumedMessages.get(0));
    Assert.assertEquals("1,first 1,1,12.34,false\r\n", consumedMessages.get(1));
    Assert.assertEquals("2,second 2,2,13.34,true\r\n", consumedMessages.get(2));
    Assert.assertEquals("3,third 3,3,14.34,false\r\n", consumedMessages.get(3));
    kafkaproducer.destroy();
  }

  @Test
  public void testTDFPublish() throws Exception {
    String testTopic = "tdf";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "TRUE", "c", "b", testTopic, "TDF");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());

    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first 1").set("c", 1).set("d", 1.0000332)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second 2").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third 3").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth 4").set("c", 4).set("d", 15.342423442424)
                .set("e", true).build());
    kafkaproducer.write(input, null);

    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
    kafkaClient.getConsumer().prepare()
      .addFromBeginning(testTopic, 0)
      .addFromBeginning(testTopic, 1)
      .addFromBeginning(testTopic, 2)
      .addFromBeginning(testTopic, 3)
      .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage msg = messages.next();
            // Add to array with partition id as index.
            consumedMessages.add(msg.getTopicPartition().getPartition(),
                                 Charsets.UTF_8.decode(msg.getPayload()).toString());
            latch.countDown();
          }
        }

        @Override
        public void finished() {
        }
      });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("4\tfourth 4\t4\t15.342423442424\ttrue\r\n", consumedMessages.get(0));
    Assert.assertEquals("1\tfirst 1\t1\t1.0000332\tfalse\r\n", consumedMessages.get(1));
    Assert.assertEquals("2\tsecond 2\t2\t13.34\ttrue\r\n", consumedMessages.get(2));
    Assert.assertEquals("3\tthird 3\t3\t14.34\tfalse\r\n", consumedMessages.get(3));
    kafkaproducer.destroy();
  }

  @Test
  public void testExcelPublish() throws Exception {
    String testTopic = "excel";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "FALSE", "c", "b", testTopic, "EXCEL");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());

    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first 1").set("c", 1).set("d", 1.0000332)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second 2").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third 3").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth 4").set("c", 4).set("d", 15.342423442424)
                .set("e", true).build());
    kafkaproducer.write(input, null);

    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
    kafkaClient.getConsumer().prepare()
      .addFromBeginning(testTopic, 0)
      .addFromBeginning(testTopic, 1)
      .addFromBeginning(testTopic, 2)
      .addFromBeginning(testTopic, 3)
      .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage msg = messages.next();
            // Add to array with partition id as index.
            consumedMessages.add(msg.getTopicPartition().getPartition(),
                                 Charsets.UTF_8.decode(msg.getPayload()).toString());
            latch.countDown();
          }
        }

        @Override
        public void finished() {
        }
      });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("4,fourth 4,4,15.342423442424,true\r\n", consumedMessages.get(0));
    Assert.assertEquals("1,first 1,1,1.0000332,false\r\n", consumedMessages.get(1));
    Assert.assertEquals("2,second 2,2,13.34,true\r\n", consumedMessages.get(2));
    Assert.assertEquals("3,third 3,3,14.34,false\r\n", consumedMessages.get(3));
    kafkaproducer.destroy();
  }

  @Test
  public void testMySQLPublish() throws Exception {
    String testTopic = "mysql";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "TRUE", "c", "b", testTopic, "MYSQL");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());

    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first 1").set("c", 1).set("d", 1.0000332)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second 2").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third 3").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth 4").set("c", 4).set("d", 15.342423442424)
                .set("e", true).build());
    kafkaproducer.write(input, null);

    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
    kafkaClient.getConsumer().prepare()
      .addFromBeginning(testTopic, 0)
      .addFromBeginning(testTopic, 1)
      .addFromBeginning(testTopic, 2)
      .addFromBeginning(testTopic, 3)
      .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage msg = messages.next();
            // Add to array with partition id as index.
            consumedMessages.add(msg.getTopicPartition().getPartition(),
                                 Charsets.UTF_8.decode(msg.getPayload()).toString());
            latch.countDown();
          }
        }

        @Override
        public void finished() {
        }
      });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("4\tfourth 4\t4\t15.342423442424\ttrue\n", consumedMessages.get(0));
    Assert.assertEquals("1\tfirst 1\t1\t1.0000332\tfalse\n", consumedMessages.get(1));
    Assert.assertEquals("2\tsecond 2\t2\t13.34\ttrue\n", consumedMessages.get(2));
    Assert.assertEquals("3\tthird 3\t3\t14.34\tfalse\n", consumedMessages.get(3));
    kafkaproducer.destroy();
  }

  @Test
  public void testRFC4180Publish() throws Exception {
    String testTopic = "rfc4180";

    KafkaProducer.Config sconfig = new KafkaProducer.Config(getBroker(), "FALSE", "c", "b", testTopic,
                                                                    "rfc4180");
    RealtimeSink<StructuredRecord> kafkaproducer = new KafkaProducer(sconfig);
    kafkaproducer.initialize(new MockRealtimeContext());

    List<StructuredRecord> input = Lists.newArrayList();
    input.add(StructuredRecord.builder(INPUT).set("a", 1L).set("b", "first 1").set("c", 1).set("d", 1.0000332)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 2L).set("b", "second 2").set("c", 2).set("d", 13.34)
                .set("e", true).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 3L).set("b", "third 3").set("c", 3).set("d", 14.34)
                .set("e", false).build());
    input.add(StructuredRecord.builder(INPUT).set("a", 4L).set("b", "fourth 4").set("c", 4).set("d", 15.342423442424)
                .set("e", true).build());
    kafkaproducer.write(input, null);

    final CountDownLatch latch = new CountDownLatch(input.size());
    final List<String> consumedMessages = new ArrayList<String>(input.size());
    kafkaClient.getConsumer().prepare()
      .addFromBeginning(testTopic, 0)
      .addFromBeginning(testTopic, 1)
      .addFromBeginning(testTopic, 2)
      .addFromBeginning(testTopic, 3)
      .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage msg = messages.next();
            // Add to array with partition id as index.
            consumedMessages.add(msg.getTopicPartition().getPartition(),
                                 Charsets.UTF_8.decode(msg.getPayload()).toString());
            latch.countDown();
          }
        }

        @Override
        public void finished() {
        }
      });
    latch.await();
    Assert.assertEquals(4L, consumedMessages.size());
    Assert.assertEquals("4\tfourth 4\t4\t15.342423442424\ttrue\r\n", consumedMessages.get(0));
    Assert.assertEquals("1\tfirst 1\t1\t1.0000332\tfalse\r\n", consumedMessages.get(1));
    Assert.assertEquals("2\tsecond 2\t2\t13.34\ttrue\r\n", consumedMessages.get(2));
    Assert.assertEquals("3\tthird 3\t3\t14.34\tfalse\r\n", consumedMessages.get(3));
    kafkaproducer.destroy();
  }  
  
  @BeforeClass
  public static void beforeClass() throws IOException {
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
  public static void afterClass() {
    kafkaServer.stopAndWait();
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  private String getBroker() {
    return "localhost:" + kafkaPort;
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    // Note: the log size properties below have been set so that we can have log rollovers
    // and log deletions in a minute.
    Properties prop = new Properties();
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
}
