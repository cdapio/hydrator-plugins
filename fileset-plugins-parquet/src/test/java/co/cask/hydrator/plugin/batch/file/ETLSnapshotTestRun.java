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

package co.cask.hydrator.plugin.batch.file;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for snapshot plugins.
 */
public class ETLSnapshotTestRun extends ETLBatchTestBase {
  private static final Schema SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("price", Schema.of(Schema.Type.INT)));

  //@Test
  public void testMultiSnapshotOutput() throws Exception {
    String tableName = "SnapshotInputTable";
    ETLStage source = new ETLStage(
      "source",
      new Plugin("Table", ImmutableMap.<String, String>builder()
        .put("name", tableName)
        .put("schema", SCHEMA.toString())
        .put("schema.row.field", "id")
        .build()));

    ETLStage sink1 = new ETLStage(
      "sink1",
      new Plugin("SnapshotParquet", ImmutableMap.<String, String>builder()
        .put("name", "snapshotParquet1")
        .put("schema", SCHEMA.toString())
        .build()));

    ETLStage sink2 = new ETLStage(
      "sink2",
      new Plugin("SnapshotAvro", ImmutableMap.<String, String>builder()
        .put("name", "snapshotParquet2")
        .put("schema", SCHEMA.toString())
        .build()));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink1)
      .addSink(sink2)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "snapshotSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // run the pipeline once with some state in the table
    DataSetManager<Table> inputManager = getDataset(tableName);
    inputManager.get().put(Bytes.toBytes("id123"), Bytes.toBytes("price"), Bytes.toBytes(777));
    inputManager.flush();

    DataSetManager<PartitionedFileSet> parquetFiles1 = getDataset("snapshotParquet1");
    DataSetManager<PartitionedFileSet> parquetFiles2 = getDataset("snapshotParquet2");
    List<DataSetManager<PartitionedFileSet>> fileSetManagers = ImmutableList.of(parquetFiles1, parquetFiles2);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Map<String, Integer> expected = new HashMap<>();
    expected.put("id123", 777);

    for (DataSetManager<PartitionedFileSet> fileSetManager : fileSetManagers) {
      fileSetManager.flush();
      Location partitionLocation = new SnapshotFileSet(fileSetManager.get()).getLocation();

      Map<String, Integer> actual = readOutput(partitionLocation);
      Assert.assertEquals(expected, actual);
    }

    // change the table contents and run the pipeline again
    inputManager.get().put(Bytes.toBytes("id456"), Bytes.toBytes("price"), Bytes.toBytes(100));
    inputManager.get().delete(Bytes.toBytes("id123"));
    inputManager.flush();

    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    expected.clear();
    expected.put("id456", 100);

    for (DataSetManager<PartitionedFileSet> fileSetManager : fileSetManagers) {
      fileSetManager.flush();
      Location partitionLocation = new SnapshotFileSet(fileSetManager.get()).getLocation();

      Map<String, Integer> actual = readOutput(partitionLocation);
      Assert.assertEquals(expected, actual);
    }

    // test snapshot sources
    testSource("snapshotParquet1", expected);
  }

  // deploys a pipeline that reads using a snapshot source and checks that it writes the expected records.
  private void testSource(String sourceName, Map<String, Integer> expected) throws Exception {
    // run another pipeline that reads from avro dataset
    ETLStage source = new ETLStage(
      "source",
      new Plugin("SnapshotParquet", ImmutableMap.<String, String>builder()
        .put("name", sourceName)
        .put("schema", SCHEMA.toString())
        .build()));

    String outputName = sourceName + "Output";
    ETLStage sink = new ETLStage(
      "sink",
      new Plugin("SnapshotParquet", ImmutableMap.<String, String>builder()
        .put("name", outputName)
        .put("schema", SCHEMA.toString())
        .build()));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "snapshotSinkTest2");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // run the pipeline, should see the 2nd state of the table
    MapReduceManager mrManager = appManager.getMapReduceManager("ETLMapReduce");
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<PartitionedFileSet> output = getDataset(outputName);
    Location partitionLocation = new SnapshotFileSet(output.get()).getLocation();
    Map<String, Integer> actual = readOutput(partitionLocation);
    Assert.assertEquals(expected, actual);
  }

  private Map<String, Integer> readOutput(Location outputLocation) throws IOException {

    Map<String, Integer> contents = new HashMap<>();
    for (Location file : outputLocation.list()) {
      String fileName = file.getName();

      if (fileName.endsWith(".parquet")) {
        Path parquetFile = new Path(file.toString());
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(parquetFile);
        GenericRecord record = reader.read();
        while (record != null) {
          contents.put(record.get("id").toString(), (Integer) record.get("price"));
          record = reader.read();
        }
      }
    }
    return contents;
  }
}

