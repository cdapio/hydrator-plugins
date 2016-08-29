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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Test;

/**
 *
 */
public class KVTableSinkTest {
  @Test(expected = IllegalArgumentException.class)
     public void testTableSinkWithMissingKeyField() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("uniqueId", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithMissingValueField() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowKey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithInvalidFieldType() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("rowKey", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testkvTableConfigure() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowKey", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithNullTypeForKey() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("rowKey", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testTableSinkWithNullableTypeForValue() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("rowKey", Schema.of(Schema.Type.STRING))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithNullableLongTypeForValue() {
    Schema inputSchema = Schema.recordOf(
      "purchase",
      // only string and bytes are supported, this is invalid type
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("rowKey", Schema.of(Schema.Type.LONG))
    );

    KVTableSink.KVTableConfig kvTableConfig = new KVTableSink.KVTableConfig("purchases", "rowKey", "user");
    KVTableSink kvTableSink = new KVTableSink(kvTableConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    kvTableSink.configurePipeline(mockPipelineConfigurer);
  }
}
