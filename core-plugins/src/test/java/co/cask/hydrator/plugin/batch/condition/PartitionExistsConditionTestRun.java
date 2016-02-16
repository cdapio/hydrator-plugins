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

package co.cask.hydrator.plugin.batch.condition;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.config.RunConditions;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PartitionExistsConditionTestRun extends ETLBatchTestBase {

  @Test
  public void testPartitionExistsCondition() throws Exception {
    String inputName = "partitionConditionInput";
    String outputName = "partitionConditionOutput";
    RunConditions runConditions = new RunConditions(120L, ImmutableList.of(
      new ETLStage(
        "partitionCheck",
        new Plugin("PartitionExists",
                   ImmutableMap.of(
                     "partition",
                     "year:${runtime:yyyy}:int," +
                       "month:${runtime:MM}:int," +
                       "day:${runtime:dd}:int," +
                       "hour:${runtime:HH}:int," +
                       "minute:${runtime:mm}:int",
                     "name", inputName,
                     "pollSeconds", "5",
                     "macroTimeOffset", "1h")))
    ));
    Schema recordSchema = Schema.recordOf("record", Schema.Field.of("i", Schema.of(Schema.Type.INT)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", 5)
      .build();
    runPipeline(inputName, outputName, runConditions, recordSchema, ImmutableList.of(record));

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputName);
    TimePartitionedFileSet newFileSet = outputManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(5, newRecords.get(0).get("i"));
  }

  @Test
  public void testTimePartitionExistsCondition() throws Exception {
    String inputName = "tpfsConditionInput";
    String outputName = "tpfsConditionOutput";
    RunConditions runConditions = new RunConditions(120L, ImmutableList.of(
      new ETLStage(
        "partitionCheck",
        new Plugin("TimePartitionExists",
                   ImmutableMap.of(
                     "partitionTime", "runtime",
                     "name", "tpfsConditionInput",
                     "pollSeconds", "5",
                     "offset", "1h")))
    ));
    Schema recordSchema = Schema.recordOf("record", Schema.Field.of("i", Schema.of(Schema.Type.INT)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", 5)
      .build();
    runPipeline(inputName, outputName, runConditions, recordSchema, ImmutableList.of(record));

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(outputName);
    TimePartitionedFileSet newFileSet = outputManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(5, newRecords.get(0).get("i"));
  }

  private void runPipeline(String inputName, String outputName,
                           RunConditions runConditions,
                           Schema schema,
                           List<GenericRecord> records) throws Exception {
    ETLStage source = new ETLStage(
      "source",
      new Plugin("TPFSParquet",
                 ImmutableMap.of(
                   Properties.TimePartitionedFileSetDataset.SCHEMA, schema.toString(),
                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, inputName,
                   Properties.TimePartitionedFileSetDataset.DELAY, "0h",
                   Properties.TimePartitionedFileSetDataset.DURATION, "1h")));
    ETLStage sink = new ETLStage(
      "sink",
      new Plugin("TPFSParquet",
                 ImmutableMap.of(
                   Properties.TimePartitionedFileSetDataset.SCHEMA, schema.toString(),
                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, outputName)));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .setRunConditions(runConditions)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // midnight jan 1 2016 UTC
    long runtime = 1451606400L * 1000L;

    // start the workflow, it shouldn't fail because it should wait until the partition exists
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("logicalStartTime", String.valueOf(runtime)));
    workflowManager.waitForStatus(true);

    // source reads from 1 hour before the runtime up to the runtime.
    // So the input partition should be from an hour before runtime.
    long inputTime = runtime - 60 * 60 * 1000;

    // write input to read
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset(inputName);
    TimePartitionOutput inputPartition = inputManager.get().getPartitionOutput(inputTime);
    Location location = inputPartition.getLocation();
    location = location.append("file.parquet");
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(Locations.toURI(location)), avroSchema);
    for (GenericRecord record : records) {
      parquetWriter.write(record);
    }
    parquetWriter.close();

    inputPartition.addPartition();
    inputManager.flush();

    // wait for run to finish
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);
  }
}
