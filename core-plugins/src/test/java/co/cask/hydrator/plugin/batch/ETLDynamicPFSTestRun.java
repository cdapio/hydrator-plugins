/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ETLDynamicPFSTestRun extends ETLBatchTestBase {

  @Test
  public void testDynamicParquetSink() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("to_part_on", Schema.of(Schema.Type.STRING))
    );

    ETLPlugin sourceConfig = new ETLPlugin("TPFSParquet",
                                           BatchSource.PLUGIN_TYPE,
                                           ImmutableMap.of(
                                             "schema", recordSchema.toString(),
                                             "name", "inputParquet",
                                             "delay", "0d",
                                             "duration", "1h"),
                                           null);
    ETLPlugin sinkConfig = new ETLPlugin("DynamicPFSParquet",
                                         BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of(
                                           "schema", recordSchema.toString(),
                                           "name", "outputParquet",
                                           "fieldNames", "to_part_on"),
                                         null);

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("DynamicPFSParquet", sinkConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "parquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    List<GenericRecord> toWrite = new ArrayList<>();
    toWrite.add(new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("to_part_on", "something_else")
      .build());
    toWrite.add(new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("to_part_on", "something")
      .build());
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("inputParquet");

    long timeInMillis = System.currentTimeMillis();
    inputManager.get().addPartition(timeInMillis, "directory");
    inputManager.flush();
    Location location = inputManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.parquet");
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(location.toURI()), avroSchema);
    for (GenericRecord record : toWrite) {
      parquetWriter.write(record);
    }
    parquetWriter.close();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<PartitionedFileSet> outputManager = getDataset("outputParquet");
    PartitionedFileSet newFileSet = outputManager.get();
    Assert.assertEquals(1, newFileSet.getPartitioning().getFields().size());
    Assert.assertTrue(newFileSet.getPartitioning().getFields().containsKey("p_to_part_on"));

    Set<PartitionDetail> partitions = newFileSet.getPartitions(PartitionFilter.ALWAYS_MATCH);
    Assert.assertEquals(2, partitions.size());

    for (int i = 0; i < partitions.size(); i++) {

      List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema,
                                                  ((PartitionDetail) partitions.toArray()[i]).getPartitionKey());
      Assert.assertEquals(1, newRecords.size());
      Assert.assertEquals(toWrite.get(i), newRecords.get(0));
    }
  }
}
