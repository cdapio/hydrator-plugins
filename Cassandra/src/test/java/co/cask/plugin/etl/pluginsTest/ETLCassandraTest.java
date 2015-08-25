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

package co.cask.plugin.etl.pluginsTest;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.plugin.etl.sink.BatchCassandraSink;
import co.cask.plugin.etl.sink.TableSink;
import co.cask.plugin.etl.source.CassandraBatchSource;
import co.cask.plugin.etl.source.StreamBatchSource;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Unit test for batch {@link BatchCassandraSink} class.
 */
public class ETLCassandraTest extends BaseETLBatchTest {
  private static final Gson GSON = new Gson();
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Cassandra.Client client;

  @Before
  public void beforeTest() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(30*1000);

    client = ConfigHelper.createConnection(new Configuration(), "localhost", 9171);
    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE KEYSPACE testkeyspace " +
                             "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"),
      Compression.NONE, ConsistencyLevel.ALL);

    client.execute_cql3_query(ByteBufferUtil.bytes("USE testkeyspace"), Compression.NONE, ConsistencyLevel.ALL);

    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE TABLE testtable ( ticker text PRIMARY KEY, price double, num int );"),
      Compression.NONE, ConsistencyLevel.ALL);
  }

  @After
  public void afterTest() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testCassandra() throws Exception {
    testCassandraSink();
    client.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE testtable;"),
                              Compression.NONE, ConsistencyLevel.ALL);
    client.execute_cql3_query(
      ByteBufferUtil.bytes("CREATE TABLE testkeyspace.testtable ( ticker text PRIMARY KEY, price double, num int );"),
      Compression.NONE, ConsistencyLevel.ALL);
    testCassandraSource();
  }

  public void testCassandraSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(StreamBatchSource.StreamProperties.NAME, STREAM_NAME)
      .put(StreamBatchSource.StreamProperties.DURATION, "10m")
      .put(StreamBatchSource.StreamProperties.DELAY, "0d")
      .put(StreamBatchSource.StreamProperties.FORMAT, Formats.CSV)
      .put(StreamBatchSource.StreamProperties.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("Cassandra", new ImmutableMap.Builder<String, String>()
      .put(BatchCassandraSink.Cassandra.INITIAL_ADDRESS, "localhost")
      .put(BatchCassandraSink.Cassandra.PORT, "9171")
      .put(BatchCassandraSink.Cassandra.PARTITIONER, "org.apache.cassandra.dht.Murmur3Partitioner")
      .put(BatchCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
      .put(BatchCassandraSink.Cassandra.COLUMN_FAMILY, "testtable")
      .put(BatchCassandraSink.Cassandra.COLUMNS, "ticker,num,price")
      .put(BatchCassandraSink.Cassandra.PRIMARY_KEY, "ticker")
      .build());

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "cassandraSinkTest");
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();

    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("SELECT * from testtable"),
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
    ETLStage source = new ETLStage("Cassandra",
                                   new ImmutableMap.Builder<String, String>()
                                     .put(CassandraBatchSource.Cassandra.INITIAL_ADDRESS, "localhost")
                                     .put(CassandraBatchSource.Cassandra.PORT, "9171")
                                     .put(CassandraBatchSource.Cassandra.PARTITIONER,
                                          "org.apache.cassandra.dht.Murmur3Partitioner")
                                     .put(CassandraBatchSource.Cassandra.KEYSPACE, "testkeyspace")
                                     .put(CassandraBatchSource.Cassandra.COLUMN_FAMILY, "testtable")
                                     .put(CassandraBatchSource.Cassandra.QUERY, "SELECT * from testtable " +
                                                                                "where token(ticker) > ? " +
                                                                                "and token(ticker) <= ?")
                                     .put(CassandraBatchSource.Cassandra.SCHEMA, BODY_SCHEMA.toString()).build());
    ETLStage sink = new ETLStage("Table",
                                 ImmutableMap.of("name", TABLE_NAME,
                                                 "schema", BODY_SCHEMA.toString(),
                                                 "schema.row.field", "ticker"));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "cassandraSourceTest");
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();

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

    Assert.assertEquals(13, (int) row1.getInt("num"));
    Assert.assertEquals(212.36, row1.getDouble("price"), 0.000001);

    manager.start();
    manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
    manager.stop();
  }
}
