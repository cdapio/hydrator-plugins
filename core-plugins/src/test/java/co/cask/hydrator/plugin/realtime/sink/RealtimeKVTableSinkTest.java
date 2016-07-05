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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.common.KVTableSinkConfig;
import co.cask.hydrator.plugin.transform.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for real time key value table sink, with valid and invalid (input, output) schemas
 */
public class RealtimeKVTableSinkTest {
  @Test(expected = IllegalArgumentException.class)
  public void testRealtimeKVTableSinkWithMissingKeyField() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("uniqueId", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig =
      new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRealtimeKVTableSinkWithMissingValueField() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowKey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig =
      new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRealtimeKVTableSinkWithInvalidFieldType() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("rowKey", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig =
      new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testRealtimeKVTableConfigure() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowKey", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig
      = new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRealtimeKVTableSinkWithNullTypeForKey() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("rowKey", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig
      = new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testRealtimeKVTableSinkWithNullableTypeForValue() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("rowKey", Schema.of(Schema.Type.STRING))
    );

    KVTableSinkConfig realtimeKVTableConfig
      = new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRealtimeKVTableSinkWithNullableLongTypeForValue() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("rowKey", Schema.of(Schema.Type.LONG))
    );

    KVTableSinkConfig realtimeKVTableConfig
      = new KVTableSinkConfig("purchases", "rowKey", "user");
    RealtimeKVTableSink realtimeKVTableSink = new RealtimeKVTableSink(realtimeKVTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    realtimeKVTableSink.configurePipeline(mockPipelineConfigurer);
  }
}
