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
import co.cask.hydrator.plugin.common.TableSinkConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test configuring Table sink with valid and invalid scenarios based on input and output schemas.
 */
public class TableSinkTest {

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithOutputSchemaExtraField() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("points", Schema.of(Schema.Type.DOUBLE))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithMissingRowKeyField() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testTableSinkWithComplexTypeSkipped() {
    // This doesn't include the rowkey in the output record. The row key inclusion in output record
    // is optional.
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithComplexTypeInOutputSchema() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithFieldTypeMismatch() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testTableSinkWithMissingOutputSchema() {
    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", null);
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testTableSink() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    TableSinkConfig tableSinkConfig = new TableSinkConfig("tableSink", "rowkey", outputSchema.toString());
    TableSink tableSink = new TableSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(outputSchema, mockPipelineConfigurer.getOutputSchema());
  }

}
