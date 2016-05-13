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
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test cases for {@link RowDenormalizerAggregator}.
 */
public class RowDenormalizerAggregatorTest extends ETLBatchTestBase {
  private Schema inputSchema = Schema.recordOf(
    "record",
    Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("FieldName", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("FieldValue", Schema.of(Schema.Type.STRING)));

  @Test
  public void testDenormalizerWithAllFields() throws Exception {
    String inputDatasetName = "denormalizer_input";
    String outputDatasetName = "denormalizer-output";

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
      .put("fieldName", "FieldName")
      .put("fieldValue", "FieldValue")
      .put("outputFields", "Firstname:," + "Lastname:," + "Address:")
      .put("schema", outputSchema.toString())
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
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalize-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("FieldName", "Firstname");
    put.add("FieldValue", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("FieldName", "Lastname");
    put.add("FieldValue", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("FieldName", "Address");
    put.add("FieldValue", "PQR place near XYZ");
    inputTable.put(put);
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Set<String> key = new HashSet<>();
    Set<String> firstName = new HashSet<>();
    Set<String> lastName = new HashSet<>();
    Set<String> address = new HashSet<>();

    for (GenericRecord record : outputRecords) {
      key.add(record.get("KeyField").toString());
      firstName.add(record.get("Firstname").toString());
      lastName.add(record.get("Lastname").toString());
      address.add(record.get("Address").toString());
    }

    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals(ImmutableSet.of("A"), key);
    Assert.assertEquals(ImmutableSet.of("ABC"), firstName);
    Assert.assertEquals(ImmutableSet.of("XYZ"), lastName);
    Assert.assertEquals(ImmutableSet.of("PQR place near XYZ"), address);
  }

  @Test
  public void testDenormalizerWithSelectedAlias() throws Exception {
    String inputDatasetName = "denormalizer_input3";
    String outputDatasetName = "denormalizer-output3";

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
      Schema.Field.of("addr", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, String> configMap = new ImmutableMap.Builder<String, String>()
      .put("keyField", "KeyField")
      .put("fieldName", "FieldName")
      .put("fieldValue", "FieldValue")
      .put("outputFields", "Firstname:," + "Address:addr")
      .put("schema", outputSchema.toString())
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
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalize-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("FieldName", "Firstname");
    put.add("FieldValue", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("FieldName", "Lastname");
    put.add("FieldValue", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("FieldName", "Address");
    put.add("FieldValue", "PQR place near XYZ");
    inputTable.put(put);
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Set<String> key = new HashSet<>();
    Set<String> firstName = new HashSet<>();
    Set<String> lastName = new HashSet<>();
    Set<String> address = new HashSet<>();

    for (GenericRecord record : outputRecords) {
      key.add(record.get("KeyField").toString());
      firstName.add(record.get("Firstname").toString());
      address.add(record.get("addr").toString());
    }

    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals(ImmutableSet.of("A"), key);
    Assert.assertEquals(ImmutableSet.of("ABC"), firstName);
    Assert.assertEquals(ImmutableSet.of("PQR place near XYZ"), address);
  }

  @Test
  public void testDenormalizerWithMultipleKeyFieldValues() throws Exception {
    String inputDatasetName = "denormalizer_input4";
    String outputDatasetName = "denormalizer-output4";

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
      .put("fieldName", "FieldName")
      .put("fieldValue", "FieldValue")
      .put("outputFields", "Firstname:," + "Address:Addr")
      .put("schema", outputSchema.toString())
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
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalize-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("FieldName", "Firstname");
    put.add("FieldValue", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("FieldName", "Lastname");
    put.add("FieldValue", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("FieldName", "Address");
    put.add("FieldValue", "PQR place near XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(4));
    put.add("KeyField", "B");
    put.add("FieldName", "Firstname");
    put.add("FieldValue", "ABC1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(5));
    put.add("KeyField", "B");
    put.add("FieldName", "Lastname");
    put.add("FieldValue", "XYZ1");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(6));
    put.add("KeyField", "B");
    put.add("FieldName", "Address");
    put.add("FieldValue", "PQR1 place near XYZ1");
    inputTable.put(put);

    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Set<String> key = new HashSet<>();
    Set<String> firstName = new HashSet<>();
    Set<String> address = new HashSet<>();

    for (GenericRecord record : outputRecords) {
      key.add(record.get("KeyField").toString());
      firstName.add(record.get("Firstname").toString());
      address.add(record.get("Addr").toString());
    }

    Assert.assertEquals(2, outputRecords.size());
    Assert.assertEquals(ImmutableSet.of("A", "B"), key);
    Assert.assertEquals(ImmutableSet.of("ABC", "ABC1"), firstName);
    Assert.assertEquals(ImmutableSet.of("PQR place near XYZ", "PQR1 place near XYZ1"), address);
  }

  @Test(expected = IllegalStateException.class)
  public void testDenormalizerWithWrongKeyFieldName() throws Exception {
    String inputDatasetName = "denormalizer_input5";
    String outputDatasetName = "denormalizer-output5";

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
      .put("keyField", "wrongkeyfield")
      .put("fieldName", "FieldName")
      .put("fieldValue", "FieldValue")
      .put("outputFields", "Firstname:," + "Lastname:," + "Address:")
      .put("schema", outputSchema.toString())
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
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalize-test");
    ApplicationManager appManager = deployApplication(appId, request);
  }

  @Test
  public void testDenormalizerWithWrongOutputField() throws Exception {
    String inputDatasetName = "denormalizer_input6";
    String outputDatasetName = "denormalizer-output6";

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
      .put("fieldName", "FieldName")
      .put("fieldValue", "FieldValue")
      .put("outputFields", "Firstname:," + "Lastname:," + "Address:," + "Salary:")
      .put("schema", outputSchema.toString())
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
    AppRequest<ETLBatchConfig> request = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalize-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // write input data
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes(1));
    put.add("KeyField", "A");
    put.add("FieldName", "Firstname");
    put.add("FieldValue", "ABC");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(2));
    put.add("KeyField", "A");
    put.add("FieldName", "Lastname");
    put.add("FieldValue", "XYZ");
    inputTable.put(put);
    put = new Put(Bytes.toBytes(3));
    put.add("KeyField", "A");
    put.add("FieldName", "Address");
    put.add("FieldValue", "PQR place near XYZ");
    inputTable.put(put);
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();
    List<GenericRecord> outputRecords = readOutput(fileSet, outputSchema);

    Set<String> key = new HashSet<>();
    Set<String> firstName = new HashSet<>();
    Set<String> lastName = new HashSet<>();
    Set<String> address = new HashSet<>();
    Set<String> salary = new HashSet<>();

    for (GenericRecord record : outputRecords) {
      key.add(record.get("KeyField").toString());
      firstName.add(record.get("Firstname").toString());
      lastName.add(record.get("Lastname").toString());
      address.add(record.get("Address").toString());
    }

    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals(ImmutableSet.of("A"), key);
    Assert.assertEquals(ImmutableSet.of("ABC"), firstName);
    Assert.assertEquals(ImmutableSet.of("XYZ"), lastName);
    Assert.assertEquals(ImmutableSet.of("PQR place near XYZ"), address);
  }

}
