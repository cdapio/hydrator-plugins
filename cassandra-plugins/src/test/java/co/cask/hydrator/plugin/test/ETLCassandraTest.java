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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.batch.sink.BatchCassandraSink;
import co.cask.hydrator.plugin.batch.source.BatchCassandraSource;
import co.cask.hydrator.plugin.realtime.RealtimeCassandraSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *  Unit test for {@link BatchCassandraSink}, {@link BatchCassandraSource},
 *  and {@link co.cask.hydrator.plugin.realtime.RealtimeCassandraSink} classes.
 */
public class ETLCassandraTest extends HydratorTestBase {
  private static final Schema SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static Cassandra.Client client;
  private static int rpcPort;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactId REALTIME_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlrealtime", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary REALTIME_APP_ARTIFACT =
    new ArtifactSummary(REALTIME_APP_ARTIFACT_ID.getArtifact(), REALTIME_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactRange REALTIME_ARTIFACT_RANGE = new ArtifactRange(Id.Namespace.DEFAULT, "etlrealtime",
                                                                                 CURRENT_VERSION, true,
                                                                                 CURRENT_VERSION, true);
  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(Id.Namespace.DEFAULT, "etlbatch",
                                                                                 CURRENT_VERSION, true,
                                                                                 CURRENT_VERSION, true);
  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);
    setupRealtimeArtifacts(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class);

    Set<ArtifactRange> parents = ImmutableSet.of(REALTIME_ARTIFACT_RANGE, BATCH_ARTIFACT_RANGE);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("cassandra-plugins", "1.0.0"),
                      parents,
                      BatchCassandraSink.class, BatchCassandraSource.class,
                      CqlInputFormat.class, CqlOutputFormat.class, ColumnFamilySplit.class,
                      RealtimeCassandraSink.class);

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
      ByteBufferUtil.bytes("CREATE TABLE testtablerealtime ( name text, graduated boolean, " +
                             "id int, score double, time bigint PRIMARY KEY );"),
      Compression.NONE, ConsistencyLevel.ALL);
  }

  @AfterClass
  public static void cleanup() throws Exception {
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
    ETLStage source = new ETLStage("source", MockSource.getPlugin("dummyInput"));
    ETLStage sink = new ETLStage("Cassandra", new ETLPlugin(
      "Cassandra",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, "TestCass")
        .put(RealtimeCassandraSink.Cassandra.ADDRESSES, "localhost:9042,invalid:abcd")
        .put(RealtimeCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(RealtimeCassandraSink.Cassandra.COLUMN_FAMILY, "testtablerealtime")
        .put(RealtimeCassandraSink.Cassandra.COLUMNS, "name,graduated,id,score,time")
        .put(RealtimeCassandraSink.Cassandra.COMPRESSION, "NONE")
        .put(RealtimeCassandraSink.Cassandra.CONSISTENCY_LEVEL, "QUORUM")
        .build(),
      null));

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
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
    String inputDatasetName = "input-batchsinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, "TestCass")
        .put(BatchCassandraSink.Cassandra.INITIAL_ADDRESS, "localhost")
        .put(BatchCassandraSink.Cassandra.PORT, Integer.toString(rpcPort))
        .put(BatchCassandraSink.Cassandra.PARTITIONER, "org.apache.cassandra.dht.Murmur3Partitioner")
        .put(BatchCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(BatchCassandraSink.Cassandra.COLUMN_FAMILY, "testtablebatch")
        .put(BatchCassandraSink.Cassandra.COLUMNS, "ticker, num, price")
        .put(BatchCassandraSink.Cassandra.PRIMARY_KEY, "ticker")
        .build();
    ETLStage sink = new ETLStage("sink", new ETLPlugin("Cassandra", BatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "cassandraSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(SCHEMA).set("ticker", "AAPL").set("num", 10).set("price", 500.32d).build(),
      StructuredRecord.builder(SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 212.36d).build()
    );
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

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

  private void testCassandraSource() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCass")
      .put(BatchCassandraSource.Cassandra.INITIAL_ADDRESS, "localhost")
      .put(BatchCassandraSource.Cassandra.PARTITIONER,
           "org.apache.cassandra.dht.Murmur3Partitioner")
      .put(BatchCassandraSource.Cassandra.KEYSPACE, "testkeyspace")
      .put(BatchCassandraSource.Cassandra.COLUMN_FAMILY, "testtablebatch")
      .put(BatchCassandraSource.Cassandra.QUERY, "SELECT * from testtablebatch " +
        "where token(ticker) > ? " +
        "and token(ticker) <= ?")
      .put(BatchCassandraSource.Cassandra.SCHEMA, SCHEMA.toString())
      .build();
    ETLStage source =
      new ETLStage("source", new ETLPlugin("Cassandra", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CassandraSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    // Verify data
    Map<Integer, Double> results = new HashMap<>();
    StructuredRecord row1 = output.get(0);
    results.put((Integer) row1.get("num"), (Double) row1.get("price"));
    StructuredRecord row2 = output.get(1);
    results.put((Integer) row2.get("num"), (Double) row2.get("price"));

    Assert.assertEquals(500.32, results.get(10), 0.000001);
    Assert.assertEquals(212.36, results.get(13), 0.000001);
  }

  private void testCassandraRealtimeSink() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("time", Schema.of(Schema.Type.LONG))
    );
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("id", 1)
        .set("name", "Bob")
        .set("score", 3.4)
        .set("graduated", false)
        .set("time", 1234567890000L)
        .build()
    );

    ETLStage source = new ETLStage("source", co.cask.cdap.etl.mock.realtime.MockSource.getPlugin(input));
    ETLStage sink = new ETLStage("Cassandra", new ETLPlugin(
      "Cassandra",
      RealtimeSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(Constants.Reference.REFERENCE_NAME, "TestCass")
        .put(RealtimeCassandraSink.Cassandra.ADDRESSES, "localhost:9042")
        .put(RealtimeCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
        .put(RealtimeCassandraSink.Cassandra.COLUMN_FAMILY, "testtablerealtime")
        .put(RealtimeCassandraSink.Cassandra.COLUMNS, "name, graduated, id, score, time")
        .put(RealtimeCassandraSink.Cassandra.COMPRESSION, "NONE")
        .put(RealtimeCassandraSink.Cassandra.CONSISTENCY_LEVEL, "QUORUM")
        .build(),
      null));
    final String cqlQuery = "select name,graduated,id,score,time from testtablerealtime";
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testESSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.class.getSimpleName());

    workerManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(cqlQuery),
                                                     Compression.NONE, ConsistencyLevel.ALL);
        return result.rows.size() > 0;
      }
    }, 30, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    workerManager.stop();

    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(cqlQuery),
                                                 Compression.NONE, ConsistencyLevel.ALL);
    List<Column> columns = result.getRows().get(0).getColumns();

    Assert.assertEquals("Bob", ByteBufferUtil.string(columns.get(0).bufferForValue()));
    byte[] bytes = new byte[] { 0 };
    Assert.assertEquals(ByteBuffer.wrap(bytes), columns.get(1).bufferForValue());
    Assert.assertEquals(1, ByteBufferUtil.toInt(columns.get(2).bufferForValue()));
    Assert.assertEquals(3.4, ByteBufferUtil.toDouble(columns.get(3).bufferForValue()), 0.000001);
    Assert.assertEquals(1234567890000L, ByteBufferUtil.toLong(columns.get(4).bufferForValue()));
  }
}
