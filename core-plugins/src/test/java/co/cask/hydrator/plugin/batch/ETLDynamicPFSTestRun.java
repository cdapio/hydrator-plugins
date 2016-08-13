/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Transform;
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
import co.cask.hydrator.plugin.common.Properties;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ETLDynamicPFSTestRun extends ETLBatchTestBase {

  @Test
  public void testDynamicPartitionParquetSink() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("a", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("b", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("c", Schema.of(Schema.Type.STRING))
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
                                           "fieldNames", "c",
                                           "partitionFormat", "column_name=value"),
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
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("a", Integer.MAX_VALUE)
      .set("b", Long.MAX_VALUE)
      .set("c", "2012-04-01")
      .build();
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("inputParquet");

    long timeInMillis = System.currentTimeMillis();
    inputManager.get().addPartition(timeInMillis, "directory");
    inputManager.flush();
    Location location = inputManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.parquet");
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(location.toURI()), avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<PartitionedFileSet> outputManager = getDataset("outputParquet");
    PartitionedFileSet newFileSet = outputManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, newRecords.get(0).get("i"));
    Assert.assertEquals(Long.MAX_VALUE, newRecords.get(0).get("l"));
  }

  private ETLBatchConfig constructTPFSETLConfig(String filesetName, String newFilesetName, Schema eventSchema) {
    ETLStage source = new ETLStage(
      "source",
      new ETLPlugin("TPFSAvro",
                    BatchSource.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                    eventSchema.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, filesetName,
                                    Properties.TimePartitionedFileSetDataset.DELAY, "0d",
                                    Properties.TimePartitionedFileSetDataset.DURATION, "2m"),
                    null));
    ETLStage sink = new ETLStage(
      "sink",
      new ETLPlugin("TPFSAvro",
                    BatchSink.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, eventSchema.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, newFilesetName),
                    null));

    ETLStage transform = new ETLStage("transform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                                 ImmutableMap.<String, String>of(), null));

    return ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();
  }
}
