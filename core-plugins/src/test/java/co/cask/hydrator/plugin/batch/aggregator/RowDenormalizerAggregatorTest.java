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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link RowDenormalizerAggregator}.
 */
public class RowDenormalizerAggregatorTest extends ETLBatchTestBase {

  private static final Schema inputSchema = Schema.recordOf(
    "record",
    Schema.Field.of("KeyField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("NameField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("ValueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  @Ignore
  @Test
  public void testDenormalizerWithMultipleKeyFieldValues() throws Exception {
    String inputDatasetName = "denormalizer_multiple_key_input";
    String outputDatasetName = "denormalizer_multiple_key_output";

    ETLStage sourceStage = new ETLStage(
      "records", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                               ImmutableMap.of(
                                 Properties.BatchReadableWritable.NAME, inputDatasetName,
                                 Properties.Table.PROPERTY_SCHEMA, inputSchema.toString()),
                               null));

    Schema outputSchema = Schema.recordOf(
      "denormalizedRecord",
      Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Addr", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> configMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "KeyField")
      .put("nameField", "NameField")
      .put("valueField", "ValueField")
      .put("outputFields", "Firstname,Address")
      .put("fieldAliases", "Address:Addr")
      .build();

    ETLStage aggregateStage = new ETLStage(
      "aggregate", new ETLPlugin("RowDenormalizer", BatchAggregator.PLUGIN_TYPE, configMap, null));


    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputDatasetName),
                            null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(aggregateStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), aggregateStage.getName())
      .addConnection(aggregateStage.getName(), sinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, "denormalize-test");

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("NameField", "Lastname");
    put.add("ValueField", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("NameField", "Address");
    put.add("ValueField", "PQR place near XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("KeyField", "B");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(5));
    put.add("KeyField", "B");
    put.add("NameField", "Lastname");
    put.add("ValueField", "XYZ1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(6));
    put.add("KeyField", "B");
    put.add("NameField", "Address");
    put.add("ValueField", "PQR1 place near XYZ1");
    inputTable.put(put);

    inputManager.flush();

    // run the pipeline
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Assert.assertEquals(2, outputRecords.size());
    for (GenericRecord record : outputRecords) {
      if ((record.get("KeyField").toString()).equals("A")) {
        Assert.assertEquals("ABC", record.get("Firstname").toString());
        Assert.assertEquals("PQR place near XYZ", record.get("Addr").toString());
      } else if ((record.get("KeyField").toString()).equals("B")) {
        Assert.assertEquals("ABC1", record.get("Firstname").toString());
        Assert.assertEquals("PQR1 place near XYZ1", record.get("Addr").toString());
      }
    }
  }

  @Ignore
  @Test
  public void testDenormalizerWithNullValues() throws Exception {
    String inputDatasetName = "denormalizer_null_values_input";
    String outputDatasetName = "denormalizer_null_values_output";

    ETLStage sourceStage = new ETLStage(
      "records", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                               ImmutableMap.of(
                                 Properties.BatchReadableWritable.NAME, inputDatasetName,
                                 Properties.Table.PROPERTY_SCHEMA, inputSchema.toString()),
                               null));

    Schema outputSchema = Schema.recordOf(
      "denormalizedRecord",
      Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Lastname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Address", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> configMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "KeyField")
      .put("nameField", "NameField")
      .put("valueField", "ValueField")
      .put("outputFields", "Firstname,Lastname,Address")
      .build();

    ETLStage aggregateStage = new ETLStage(
      "aggregate", new ETLPlugin("RowDenormalizer", BatchAggregator.PLUGIN_TYPE, configMap, null));


    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputDatasetName),
                            null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(aggregateStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), aggregateStage.getName())
      .addConnection(aggregateStage.getName(), sinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, "denormalize-test-with-null");

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("NameField", (byte[]) null);
    put.add("ValueField", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("NameField", "Address");
    put.add("ValueField", "PQR place near XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("KeyField", "B");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(5));
    put.add("KeyField", "B");
    put.add("NameField", "Lastname");
    put.add("ValueField", "XYZ1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(6));
    put.add("KeyField", "B");
    put.add("NameField", "Address");
    put.add("ValueField", (byte[]) null);
    inputTable.put(put);
    put = new Put(Bytes.toBytes(7));
    put.add("KeyField", "C");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC2");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(8));
    put.add("KeyField", (byte[]) null);
    put.add("NameField", "Lastname");
    put.add("ValueField", "XYZ2");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(9));
    put.add("KeyField", "C");
    put.add("NameField", (byte[]) null);
    put.add("ValueField", (byte[]) null);
    inputTable.put(put);

    inputManager.flush();

    // run the pipeline
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Assert.assertEquals(3, outputRecords.size());
    for (GenericRecord record : outputRecords) {
      if ((record.get("KeyField").toString()).equals("A")) {
        Assert.assertEquals("ABC", record.get("Firstname").toString());
        Assert.assertEquals("PQR place near XYZ", record.get("Address").toString());
      } else if ((record.get("KeyField").toString()).equals("B")) {
        Assert.assertEquals("ABC1", record.get("Firstname").toString());
        Assert.assertEquals("XYZ1", record.get("Lastname").toString());
      } else if ((record.get("KeyField").toString()).equals("C")) {
        Assert.assertEquals("ABC2", record.get("Firstname").toString());
      }
    }
  }

  @Ignore
  @Test
  public void testDenormalizerWithWrongOutputField() throws Exception {
    String inputDatasetName = "denormalizer_wrong_field_input";
    String outputDatasetName = "denormalizer_wrong_field_output";

    ETLStage sourceStage = new ETLStage(
      "records", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                               ImmutableMap.of(
                                 Properties.BatchReadableWritable.NAME, inputDatasetName,
                                 Properties.Table.PROPERTY_SCHEMA, inputSchema.toString()),
                               null));

    Schema outputSchema = Schema.recordOf(
      "denormalizedRecord",
      Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Lastname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Address", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Salary", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> configMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "KeyField")
      .put("nameField", "NameField")
      .put("valueField", "ValueField")
      .put("outputFields", "Firstname,Lastname,Address,Salary")
      .build();

    ETLStage aggregateStage = new ETLStage(
      "aggregate", new ETLPlugin("RowDenormalizer", BatchAggregator.PLUGIN_TYPE, configMap, null));


    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputDatasetName),
                            null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(aggregateStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), aggregateStage.getName())
      .addConnection(aggregateStage.getName(), sinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, "denormalize-test-with-wrong-output");

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("NameField", "Firstname");
    put.add("ValueField", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("NameField", "Lastname");
    put.add("ValueField", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("NameField", "Address");
    put.add("ValueField", "PQR place near XYZ");
    inputTable.put(put);
    inputManager.flush();

    // run the pipeline
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals("A", outputRecords.get(0).get("KeyField").toString());
    Assert.assertEquals("ABC", outputRecords.get(0).get("Firstname").toString());
    Assert.assertEquals("XYZ", outputRecords.get(0).get("Lastname").toString());
    Assert.assertEquals("PQR place near XYZ", outputRecords.get(0).get("Address").toString());
  }
}
