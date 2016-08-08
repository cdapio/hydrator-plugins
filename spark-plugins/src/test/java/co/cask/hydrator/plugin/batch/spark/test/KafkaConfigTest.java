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

package co.cask.hydrator.plugin.batch.spark.test;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.spark.KafkaConfig;
import org.junit.Assert;
import org.junit.Test;

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
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "brokers", "topics", schema.toString(), null, "ts", "key");
    Schema expected = Schema.recordOf("kafka.record", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
    Schema actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);

    // test with no ts field or key field
    schema = Schema.recordOf("rec", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
    config = new KafkaConfig("name", "brokers", "topics", schema.toString(), null, null, null);
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
    config = new KafkaConfig("name", "brokers", "topics", schema.toString(), "csv", "ts", "key");
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
    config = new KafkaConfig("name", "brokers", "topics", schema.toString(), "csv", null, null);
    actual = config.getMessageSchema();
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameTimestampAndKeyErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("key", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "brokers", "topics", schema.toString(), null, "ts", "ts");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleMessageFieldsErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("f1", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f2", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "brokers", "topics", schema.toString(), null, null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFormatErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("f1", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f2", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "brokers", "topics", schema.toString(), "badformat", null, null);
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSpecialFieldsErrors() {
    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES))
    );
    KafkaConfig config = new KafkaConfig("name", "brokers", "topics", schema.toString(), null, "ts", "key");
    config.validate();
  }
}
