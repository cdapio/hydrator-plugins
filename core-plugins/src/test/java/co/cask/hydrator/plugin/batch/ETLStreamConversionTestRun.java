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

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * Tests for {@link ETLBatchApplication} for Stream conversion from stream to avro format for writing to
 * {@link TimePartitionedFileSet}
 */
public class ETLStreamConversionTestRun extends ETLBatchTestBase {
  private static final Gson GSON = new Gson();

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Schema EVENT_SCHEMA = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @Ignore
  @Test
  public void testStreamConversionTPFSParquetSink() throws Exception {
    testSink(Engine.MAPREDUCE, "TPFSParquet");
    testSink(Engine.SPARK, "TPFSParquet");
  }

  @Ignore
  @Test
  public void testStreamConversionTPFSAvroSink() throws Exception {
    testSink(Engine.MAPREDUCE, "TPFSAvro");
    testSink(Engine.SPARK, "TPFSAvro");
  }

  private void testSink(Engine engine, String sinkType) throws Exception {
    String streamName = String.format("stream_%s_%s", sinkType, engine.name());
    String filesetName = String.format("converted_%s_%s", sinkType, engine.name());
    StreamManager streamManager = getStreamManager(streamName);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");

    ETLBatchConfig etlConfig = constructETLBatchConfig(engine, streamName, filesetName, sinkType);

    ApplicationManager appManager = deployETL(etlConfig, String.format("app_%s_%s", engine, sinkType));
    runETLOnce(appManager);

    // get the output fileset, and read the parquet/avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, EVENT_SCHEMA);
    Assert.assertEquals(1, records.size());

    try (Connection sqlConn = getQueryClient(NamespaceId.DEFAULT);
         ResultSet resultSet = sqlConn.prepareStatement(String.format("select * from dataset_%s", filesetName))
           .executeQuery()) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(GSON.toJson(ImmutableMap.of("header1", "bar")), resultSet.getString(2));
      Assert.assertEquals("AAPL", resultSet.getString(3));
      Assert.assertEquals(10, resultSet.getInt(4));
      Assert.assertEquals(500.32, resultSet.getDouble(5), 0.0001);
      Assert.assertFalse(resultSet.next());
    }
  }

  private ETLBatchConfig constructETLBatchConfig(Engine engine, String streamName,
                                                 String fileSetName, String sinkType) {
    ETLPlugin sourceConfig = new ETLPlugin(
      "Stream",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(Properties.Stream.NAME, streamName)
        .put(Properties.Stream.DURATION, "10m")
        .put(Properties.Stream.DELAY, "0d")
        .put(Properties.Stream.FORMAT, Formats.CSV)
        .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
        .put("format.setting.delimiter", "|")
        .build(),
      null);
    ETLStage source  = new ETLStage("source", sourceConfig);
    ETLPlugin sinkConfig = new ETLPlugin(
      sinkType,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, EVENT_SCHEMA.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, fileSetName),
      null);
    ETLStage sink = new ETLStage("sink", sinkConfig);
    ETLPlugin transformConfig = new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                              ImmutableMap.<String, String>of(), null);
    ETLStage transform = new ETLStage("transforms", transformConfig);
    return ETLBatchConfig.builder("* * * * *")
      .setEngine(engine)
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();
  }
}
