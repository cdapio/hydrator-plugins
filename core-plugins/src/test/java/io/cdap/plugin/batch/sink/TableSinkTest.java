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

package io.cdap.plugin.batch.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.common.Properties;
import io.cdap.plugin.common.TableSinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test configuring Table sink with valid and invalid scenarios based on input and output schemas.
 */
public class TableSinkTest extends ETLBatchTestBase {

  @Test
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
    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
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
    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testTableSinkWithComplexTypeSkipped() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
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

  @Test
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
    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
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
    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
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
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
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

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithOutputSchemaWithOnlyRowKeyField() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING))
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
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithOutputSchemaWithOneNonRowKeyField() {
    Schema outputSchema = Schema.recordOf(
        "purchase",
        Schema.Field.of("user", Schema.of(Schema.Type.STRING))
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
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableSinkWithOutputSchemaMissingRowKeyField() {
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
  }

  @Test
  public void testTableSinkWithMacro() throws Exception {
    Schema schema = Schema.recordOf(
      "action",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );
    ETLPlugin sourceConfig = new ETLPlugin("Table",
                                           BatchSource.PLUGIN_TYPE,
                                           ImmutableMap.of(
                                             Properties.BatchReadableWritable.NAME, "TableSinkInputTable",
                                             Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                             Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                                           null);
    ETLStage source = new ETLStage("tableSource", sourceConfig);
    ETLPlugin sinkConfig = new ETLPlugin("Table",
                                         BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of(Properties.Table.NAME, "${name}",
                                                         Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey"),
                                         null);
    ETLStage sink = new ETLStage("tableSinkUnique", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    ApplicationManager appManager = deployETL(etlConfig, "testTableSink");
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("TableSinkInputTable");
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();
    Map<String, String> runTimeProperties = new HashMap<String, String>();
    runTimeProperties.put("name", "tableSinkName");
    runETLOnce(appManager, runTimeProperties);
    // verify
    DataSetManager<Table> tableManager = getDataset("tableSinkName");
    Table table = tableManager.get();
    Assert.assertEquals("samuel", table.get(Bytes.toBytes("row1")).getString("user"));
  }
}
