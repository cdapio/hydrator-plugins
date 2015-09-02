package co.cask.plugin.etl.realtime.pluginsTest;

import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.batch.sink.BatchElasticsearchSink;
import co.cask.cdap.template.etl.batch.source.ElasticsearchSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.realtime.ETLRealtimeTemplate;
import co.cask.cdap.template.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.template.etl.transform.ScriptFilterTransform;
import co.cask.cdap.template.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.TestBase;
import co.cask.plugin.etl.realtime.RealtimeCassandraSink;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.cassandra.hadoop.ConfigHelper;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *  Unit test for batch {@link BatchElasticsearchSink} and {@link ElasticsearchSource} classes.
 */
public class ETLCassandraRealtimeTest extends TestBase {
  private static final Id.Namespace NAMESPACE = Id.Namespace.DEFAULT;
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLRealtime");
  private static final Gson GSON = new Gson();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Cassandra.Client client;
  private int rpcPort;

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
      ByteBufferUtil.bytes("CREATE TABLE testtable ( name text PRIMARY KEY, graduated boolean, " +
                             "id int, score double, time bigint );"),
      Compression.NONE, ConsistencyLevel.ALL);
  }

  @BeforeClass
  public static void setupTests() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "realtime-sources-1.0.0.jar",
                       DataGeneratorSource.class);
    addTemplatePlugins(TEMPLATE_ID, "realtime-sinks-1.0.0.jar",
                       RealtimeCassandraSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar",
                       ProjectionTransform.class, ScriptFilterTransform.class,
                       StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLRealtimeTemplate.class,
                   PipelineConfigurable.class.getPackage().getName(),
                   ETLStage.class.getPackage().getName(),
                   RealtimeSource.class.getPackage().getName());
  }

  @After
  public void afterTest() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testCassandraRealtimeSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                                                                    DataGeneratorSource.TABLE_TYPE));
    ETLStage sink = new ETLStage("Cassandra",
                                 new ImmutableMap.Builder<String, String>()
                                   .put(RealtimeCassandraSink.Cassandra.ADDRESSES, "localhost:9042")
                                   .put(RealtimeCassandraSink.Cassandra.KEYSPACE, "testkeyspace")
                                   .put(RealtimeCassandraSink.Cassandra.COLUMN_FAMILY, "testtable")
                                   .put(RealtimeCassandraSink.Cassandra.COLUMNS, "name,graduated,id,score,time")
                                   .put(RealtimeCassandraSink.Cassandra.COMPRESSION, "NONE")
                                   .put(RealtimeCassandraSink.Cassandra.CONSISTENCY_LEVEL, "QUORUM")
                                   .build());
    List<ETLStage> transforms = new ArrayList<>();
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "cassandraRealtimeSinkTest");
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("select * from testtable"),
                                  Compression.NONE, ConsistencyLevel.ALL);
        return result.rows.size();
      }
    }, 30, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    manager.stop();

    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes("select * from testtable"),
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
