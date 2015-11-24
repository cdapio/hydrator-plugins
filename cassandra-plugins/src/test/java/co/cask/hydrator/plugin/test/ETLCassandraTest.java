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

package co.cask.hydrator.plugin.test;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.batch.sink.TableSink;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.etl.transform.ProjectionTransform;
import co.cask.cdap.etl.transform.ScriptFilterTransform;
import co.cask.cdap.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.plugin.batch.sink.BatchCassandraSink;
import co.cask.hydrator.plugin.batch.source.BatchCassandraSource;
import co.cask.hydrator.plugin.realtime.RealtimeCassandraSink;
import co.cask.hydrator.plugin.testclasses.StreamBatchSource;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *  Unit test for {@link BatchCassandraSink}, {@link BatchCassandraSource},
 *  and {@link co.cask.hydrator.plugin.realtime.RealtimeCassandraSink} classes.
 */
public class ETLCassandraTest extends TestBase {
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private Cassandra.Client client;
  private int rpcPort;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final Id.Artifact BATCH_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                            "etlbatch", CURRENT_VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT = ArtifactSummary.from(BATCH_APP_ARTIFACT_ID);

  private static final Id.Artifact REALTIME_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                               "etlrealtime", "3.2.0");
  private static final ArtifactSummary REALTIME_APP_ARTIFACT = ArtifactSummary.from(REALTIME_APP_ARTIFACT_ID);

  private static final ArtifactRange REALTIME_ARTIFACT_RANGE = new ArtifactRange(Id.Namespace.DEFAULT, "etlrealtime",
                                                                                 CURRENT_VERSION, true,
                                                                                 CURRENT_VERSION, true);
  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(Id.Namespace.DEFAULT, "etlbatch",
                                                                                 CURRENT_VERSION, true,
                                                                                 CURRENT_VERSION, true);
  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add artifact for batch sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      BatchCassandraSink.class, BatchCassandraSource.class, StreamBatchSource.class, TableSink.class,
                      CqlInputFormat.class, CqlOutputFormat.class, ColumnFamilySplit.class);

    //add the artifact for the etl realtime app
    addAppArtifact(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add artifact for realtime sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "realtime-sources", "1.0.0"), REALTIME_APP_ARTIFACT_ID,
                      DataGeneratorSource.class);
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "realtime-sinks", "1.0.0"), REALTIME_APP_ARTIFACT_ID,
                      RealtimeCassandraSink.class);

    // add artifact for transforms
    Set<ArtifactRange> artifactRanges = new HashSet<>();
    artifactRanges.add(REALTIME_ARTIFACT_RANGE);
    artifactRanges.add(BATCH_ARTIFACT_RANGE);
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "transforms", "1.0.0"), artifactRanges,
                      ProjectionTransform.class, ScriptFilterTransform.class,
                      StructuredRecordToGenericRecordTransform.class);
  }

  @Before
  public void beforeTest() throws Exception {
    rpcPort = 9160;
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra210.yaml", 30 * 1000);

    client = ConfigHelper.createConnection(new Configuration(), "localhost", rpcPort);
    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE KEYSPACE testkeyspace " +
                             "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"),
      Compression.NONE, ConsistencyLevel.ALL);

    client.execute_cql3_query(ByteBufferUtil.bytes("USE testkeyspace"), Compression.NONE, ConsistencyLevel.ALL);
    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE TABLE testtablebatch ( ticker text PRIMARY KEY, price double, num int );"),
      Compression.NONE, ConsistencyLevel.ALL);
    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE TABLE testtablerealtime ( name text PRIMARY KEY, graduated boolean, " +
                             "id int, score double, time bigint );"),
      Compression.NONE, ConsistencyLevel.ALL);
  }

  @After
  public void afterTest() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testCassandra() throws Exception {
    testCassandraRealtimeSink();
    testCassandraSink();
    testCassandraSource();
  }

  @Test
  public void testInvalidRealtimeCassandraSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", new Plugin(
      "DataGenerator",
      ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                      DataGeneratorSource.TABLE_TYPE)));
    ETLStage sink = new ETLStage("Cassandra", new Plugin(
      "Cassandra",
      new ImmutableMap.Builder<String, String>()
        .put(RealtimeCassandraSink.Cassandra.ADDRESSES, "localhost:9042,invalid:abcd")
        .put(RealtimeCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(RealtimeCassandraSink.Cassandra.COLUMN_FAMILY, "testtablerealtime")
        .put(RealtimeCassandraSink.Cassandra.COLUMNS, "name,graduated,id,score,time")
        .put(RealtimeCassandraSink.Cassandra.COMPRESSION, "NONE")
        .put(RealtimeCassandraSink.Cassandra.CONSISTENCY_LEVEL, "QUORUM")
        .build()));
    List<ETLStage> transforms = new ArrayList<>();
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testESSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  public void testCassandraSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", new Plugin(
      "Stream",
      ImmutableMap.<String, String>builder()
        .put(Properties.Stream.NAME, STREAM_NAME)
        .put(Properties.Stream.DURATION, "10m")
        .put(Properties.Stream.DELAY, "0d")
        .put(Properties.Stream.FORMAT, Formats.CSV)
        .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
        .put("format.setting.delimiter", "|")
        .build()));

    ETLStage sink = new ETLStage("Cassandra", new Plugin(
      "Cassandra",
      new ImmutableMap.Builder<String, String>()
        .put(BatchCassandraSink.Cassandra.INITIAL_ADDRESS, "localhost")
        .put(BatchCassandraSink.Cassandra.PORT, Integer.toString(rpcPort))
        .put(BatchCassandraSink.Cassandra.PARTITIONER, "org.apache.cassandra.dht.Murmur3Partitioner")
        .put(BatchCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(BatchCassandraSink.Cassandra.COLUMN_FAMILY, "testtablebatch")
        .put(BatchCassandraSink.Cassandra.COLUMNS, "ticker, num, price")
        .put(BatchCassandraSink.Cassandra.PRIMARY_KEY, "ticker")
        .build()));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "cassandraSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("SELECT * from testtablebatch"),
                                                 Compression.NONE, ConsistencyLevel.ALL);
    Assert.assertEquals(2, result.getRowsSize());
    Assert.assertEquals(3, result.getRows().get(0).getColumns().size());

    //first entry - "AAPL"
    Assert.assertEquals(ByteBufferUtil.bytes("ticker"), result.getRows().get(0).getColumns().get(0).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes("AAPL"), result.getRows().get(0).getColumns().get(0).bufferForValue());
    Assert.assertEquals(ByteBufferUtil.bytes("num"), result.getRows().get(0).getColumns().get(1).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes(10), result.getRows().get(0).getColumns().get(1).bufferForValue());
    Assert.assertEquals(ByteBufferUtil.bytes("price"), result.getRows().get(0).getColumns().get(2).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes(500.32), result.getRows().get(0).getColumns().get(2).bufferForValue());

    //second entry - "CDAP"
    Assert.assertEquals(ByteBufferUtil.bytes("ticker"), result.getRows().get(1).getColumns().get(0).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes("CDAP"), result.getRows().get(1).getColumns().get(0).bufferForValue());
    Assert.assertEquals(ByteBufferUtil.bytes("num"), result.getRows().get(1).getColumns().get(1).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes(13), result.getRows().get(1).getColumns().get(1).bufferForValue());
    Assert.assertEquals(ByteBufferUtil.bytes("price"), result.getRows().get(1).getColumns().get(2).bufferForName());
    Assert.assertEquals(ByteBufferUtil.bytes(212.36), result.getRows().get(1).getColumns().get(2).bufferForValue());
  }

  @SuppressWarnings("ConstantConditions")
  private void testCassandraSource() throws Exception {
    ETLStage source = new ETLStage("Cassandra", new Plugin(
      "Cassandra",
      new ImmutableMap.Builder<String, String>()
        .put(BatchCassandraSource.Cassandra.INITIAL_ADDRESS, "localhost")
        .put(BatchCassandraSource.Cassandra.PARTITIONER,
             "org.apache.cassandra.dht.Murmur3Partitioner")
        .put(BatchCassandraSource.Cassandra.KEYSPACE, "testkeyspace")
        .put(BatchCassandraSource.Cassandra.COLUMN_FAMILY, "testtablebatch")
        .put(BatchCassandraSource.Cassandra.QUERY, "SELECT * from testtablebatch " +
          "where token(ticker) > ? " +
          "and token(ticker) <= ?")
        .put(BatchCassandraSource.Cassandra.SCHEMA, BODY_SCHEMA.toString())
        .build()));
    ETLStage sink = new ETLStage("Table", new Plugin(
      "Table",
      ImmutableMap.of(Properties.Table.NAME, TABLE_NAME,
                      Properties.Table.PROPERTY_SCHEMA, BODY_SCHEMA.toString(),
                      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker")));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CassandraSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(TABLE_NAME);
    Table outputTable = outputManager.get();

    // Scanner to verify number of rows
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Assert.assertNotNull(row1);
    Row row2 = scanner.next();
    Assert.assertNotNull(row2);
    Assert.assertNull(scanner.next());
    scanner.close();

    // Verify data
    Assert.assertEquals(10, (int) row1.getInt("num"));
    Assert.assertEquals(500.32, row1.getDouble("price"), 0.000001);
    Assert.assertNull(row1.get("NOT_IMPORTED"));

    Assert.assertEquals(13, (int) row2.getInt("num"));
    Assert.assertEquals(212.36, row2.getDouble("price"), 0.000001);
  }

  private void testCassandraRealtimeSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", new Plugin(
      "DataGenerator",
      ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE)));
    ETLStage sink = new ETLStage("Cassandra", new Plugin(
      "Cassandra",
      new ImmutableMap.Builder<String, String>()
        .put(RealtimeCassandraSink.Cassandra.ADDRESSES, "localhost:9042")
        .put(RealtimeCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(RealtimeCassandraSink.Cassandra.COLUMN_FAMILY, "testtablerealtime")
        .put(RealtimeCassandraSink.Cassandra.COLUMNS, "name, graduated, id, score, time")
        .put(RealtimeCassandraSink.Cassandra.COMPRESSION, "NONE")
        .put(RealtimeCassandraSink.Cassandra.CONSISTENCY_LEVEL, "QUORUM")
        .build()));
    List<ETLStage> transforms = new ArrayList<>();
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testESSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.class.getSimpleName());

    workerManager.start();
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("select * from testtablerealtime"),
                                                     Compression.NONE, ConsistencyLevel.ALL);
        return result.rows.size();
      }
    }, 30, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    workerManager.stop();

    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("select * from testtablerealtime"),
                                                 Compression.NONE, ConsistencyLevel.ALL);
    List<Column> columns = result.getRows().get(0).getColumns();

    Assert.assertEquals("Bob", ByteBufferUtil.string(columns.get(0).bufferForValue()));
    byte[] bytes = new byte[1];
    bytes[0] = 0;
    Assert.assertEquals(ByteBuffer.wrap(bytes), columns.get(1).bufferForValue());
    Assert.assertEquals(1, ByteBufferUtil.toInt(columns.get(2).bufferForValue()));
    Assert.assertEquals(3.4, ByteBufferUtil.toDouble(columns.get(3).bufferForValue()), 0.000001);
    Assert.assertNotNull(ByteBufferUtil.toLong(columns.get(4).bufferForValue()));
  }
}
