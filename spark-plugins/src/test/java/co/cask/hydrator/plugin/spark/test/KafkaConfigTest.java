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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.spark.KafkaConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import kafka.common.TopicAndPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for KafkaConfig
 */
public class KafkaConfigTest {

  @Test
  public void testGetMessageSchema() {
    // test with null format, which means the message will just be bytes
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("partition", Schema.of(Schema.Type.INT)),
      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), null,
                                         "ts", "key", "partition", "offset");
    Schema expected = Schema.recordOf("kafka.record", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
    Schema actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);

    // test with no ts field or key field
    schema = Schema.recordOf("rec", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
    config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), null, null, null, null, null);
    expected = Schema.recordOf("kafka.record", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
    actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);

    // test with csv format
    schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), "csv", "ts", "key", null, null);
    // message schema should have ts and key stripped, since they are the timestamp and key fields.
    expected =  Schema.recordOf(
      "kafka.record",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);

    // try without a timestamp or key field, which means 'ts' and 'key' should be interpreted as fields in the message.
    expected = schema;
    config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), "csv", null, null, null, null);
    actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetBrokerMap() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092,host2:9093", "topic",
                                         schema.toString(), null, "ts", "key", null, null);
    Map<String, Integer> expected = ImmutableMap.of("host1", 9092, "host2", 9093);
    Assert.assertEquals(expected, config.getBrokerMap());
  }

  @Test
  public void testGetInitialOffsets() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", "1,3,5,7",
                                         "1:0,3:35,7:90", -1L, schema.toString(), null,
                                         "ts", "key", null, null);
    Map<TopicAndPartition, Long> expected = ImmutableMap.of(
      new TopicAndPartition("topic", 1), 0L,
      new TopicAndPartition("topic", 3), 35L,
      new TopicAndPartition("topic", 5), -1L,
      new TopicAndPartition("topic", 7), 90L);
    Assert.assertEquals(expected, config.getInitialPartitionOffsets(ImmutableSet.of(1, 3, 5, 7)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBrokersErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092,host2:", "topic", schema.toString(), null,
                                         "ts", "key", null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameTimestampAndKeyErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), null,
                                         "ts", "ts", null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleMessageFieldsErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("f1", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f2", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), null,
                                         null, null, null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFormatErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("f1", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f2", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), "badformat",
                                         null, null, null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSpecialFieldsErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "host1:9092", "topic", schema.toString(), null,
                                         "ts", "key", null, null);
    config.validate();
  }
}
