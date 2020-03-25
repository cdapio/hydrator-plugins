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

package io.cdap.plugin.batch.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.common.Properties;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link RowDenormalizerAggregator}.
 */
public class RowDenormalizerAggregatorTest extends ETLBatchTestBase {

  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("KeyField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("NameField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("ValueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private List<GenericRecord> testHelper(String appName, String inputDatasetName, List<StructuredRecord> input,
                                         Schema outputSchema, ETLStage sourceStage, ETLStage aggregateStage,
                                         String outputDatasetName, ETLStage sinkStage,
                                         Map<String, String> runTimeProperties) throws Exception {
    ETLBatchConfig config = ETLBatchConfig.builder()
            .addStage(sourceStage)
            .addStage(aggregateStage)
            .addStage(sinkStage)
            .addConnection(sourceStage.getName(), aggregateStage.getName())
            .addConnection(aggregateStage.getName(), sinkStage.getName())
            .build();
    ApplicationManager appManager = deployETL(config, appName);

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

    // run the pipeline
    runETLOnce(appManager, runTimeProperties);
    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    return readOutput(fileSet, outputSchema);
  }

  @Test
  public void testDenormalizerWithMultipleKeyFieldValues() throws Exception {
    String inputDatasetName = "denormalizer_multiple_key_input";
    String outputDatasetName = "denormalizer_multiple_key_output";
    String appName = "denormalize-test";

    ETLStage sourceStage = new ETLStage("records", MockSource.getPlugin(inputDatasetName, INPUT_SCHEMA));

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

    List<StructuredRecord> input = ImmutableList.of(
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Lastname")
                    .set("ValueField", "XYZ")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Address")
                    .set("ValueField", "PQR place near XYZ")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "B")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC1")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "B")
                    .set("NameField", "Lastname")
                    .set("ValueField", "XYZ1")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "B")
                    .set("NameField", "Address")
                    .set("ValueField", "PQR1 place near XYZ1")
                    .build());
    List<GenericRecord> outputRecords = testHelper(appName, inputDatasetName, input, outputSchema, sourceStage,
            aggregateStage, outputDatasetName, sinkStage, ImmutableMap.of());

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

  @Test
  public void testDenormalizerWithMacro() throws Exception {
    String inputDatasetName = "denormalizer_unknown_inputschema_input";
    String outputDatasetName = "denormalizer_unknown_inputschema_output";
    String appName = "denormalize-test-unknown-inputschema";

    ETLStage sourceStage = new ETLStage("records", MockSource.getPlugin(inputDatasetName));

    Schema outputSchema = Schema.recordOf(
      "denormalizedRecord",
      Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("Addr", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> macroMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "${keyField}")
      .put("nameField", "${nameField}")
      .put("valueField", "${valueField}")
      .put("outputFields", "${outputFields}")
      .put("fieldAliases", "${fieldAliases}")
      .build();
    Map<String, String> configMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "KeyField")
      .put("nameField", "NameField")
      .put("valueField", "ValueField")
      .put("outputFields", "Firstname,Address")
      .put("fieldAliases", "Address:Addr")
      .build();

    ETLStage aggregateStage = new ETLStage(
      "aggregate", new ETLPlugin("RowDenormalizer", BatchAggregator.PLUGIN_TYPE, macroMap, null));


    ETLStage sinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
        Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputDatasetName),
      null));

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "A")
        .set("NameField", "Firstname")
        .set("ValueField", "ABC")
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "A")
        .set("NameField", "Lastname")
        .set("ValueField", "XYZ")
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "A")
        .set("NameField", "Address")
        .set("ValueField", "PQR place near XYZ")
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "B")
        .set("NameField", "Firstname")
        .set("ValueField", "ABC1")
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "B")
        .set("NameField", "Lastname")
        .set("ValueField", "XYZ1")
        .build(),
      StructuredRecord.builder(INPUT_SCHEMA)
        .set("KeyField", "B")
        .set("NameField", "Address")
        .set("ValueField", "PQR1 place near XYZ1")
        .build());
    List<GenericRecord> outputRecords = testHelper(appName, inputDatasetName, input, outputSchema, sourceStage,
      aggregateStage, outputDatasetName, sinkStage, configMap);

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

  @Test
  public void testDenormalizerWithNullValues() throws Exception {
    String inputDatasetName = "denormalizer_null_values_input";
    String outputDatasetName = "denormalizer_null_values_output";
    String appName = "denormalize-test-with-null";

    ETLStage sourceStage = new ETLStage("records", MockSource.getPlugin(inputDatasetName, INPUT_SCHEMA));

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
    List<StructuredRecord> input = ImmutableList.of(
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Lastname")
                    .set("ValueField", "XYZ")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Address")
                    .set("ValueField", "PQR place near XYZ")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "B")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC1")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "B")
                    .set("NameField", "Lastname")
                    .set("ValueField", "XYZ1")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "C")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC2")
                    .build());
    List<GenericRecord> outputRecords = testHelper(appName, inputDatasetName, input, outputSchema, sourceStage,
            aggregateStage, outputDatasetName, sinkStage, ImmutableMap.of());


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

  @Test
  public void testDenormalizerWithWrongOutputField() throws Exception {
    String inputDatasetName = "denormalizer_wrong_field_input";
    String outputDatasetName = "denormalizer_wrong_field_output";
    String appName = "denormalize-test-with-wrong-output";
    ETLStage sourceStage = new ETLStage("records", MockSource.getPlugin(inputDatasetName, INPUT_SCHEMA));

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
    List<StructuredRecord> input = ImmutableList.of(
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Firstname")
                    .set("ValueField", "ABC")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Lastname")
                    .set("ValueField", "XYZ")
                    .build(),
            StructuredRecord.builder(INPUT_SCHEMA)
                    .set("KeyField", "A")
                    .set("NameField", "Address")
                    .set("ValueField", "PQR place near XYZ")
                    .build());

    List<GenericRecord> outputRecords = testHelper(appName, inputDatasetName, input, outputSchema, sourceStage,
            aggregateStage, outputDatasetName, sinkStage, ImmutableMap.of());

    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals("A", outputRecords.get(0).get("KeyField").toString());
    Assert.assertEquals("ABC", outputRecords.get(0).get("Firstname").toString());
    Assert.assertEquals("XYZ", outputRecords.get(0).get("Lastname").toString());
    Assert.assertEquals("PQR place near XYZ", outputRecords.get(0).get("Address").toString());
  }
}
