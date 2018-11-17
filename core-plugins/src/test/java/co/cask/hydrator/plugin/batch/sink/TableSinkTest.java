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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.TableSinkConfig;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test configuring Table sink with valid and invalid scenarios based on input and output schemas.
 */
public class TableSinkTest extends ETLBatchTestBase {

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

  @Ignore
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
