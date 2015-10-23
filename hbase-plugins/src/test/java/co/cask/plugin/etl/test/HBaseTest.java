package co.cask.plugin.etl.test;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.ETLMapReduce;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.plugin.etl.batch.sink.HBaseSink;
import co.cask.plugin.etl.batch.source.HBaseSource;
import co.cask.plugin.etl.testclasses.StreamBatchSource;
import co.cask.plugin.etl.testclasses.TableSink;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HBaseTest extends TestBase {
  private static final String STREAM_NAME = "someStream";
  private static final String TABLE_NAME = "outputTable";
  private static final String HBASE_TABLE_NAME = "input";
  private static final String HBASE_FAMILY_COLUMN = "col";
  private static final String ROW1 = "row1";
  private static final String ROW2 = "row2";
  private static final String COL1 = "col1";
  private static final String COL2 = "col2";
  private static final String VAL1 = "val1";
  private static final String VAL2 = "val2";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("col1", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("col2", Schema.of(Schema.Type.STRING)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final Id.Artifact BATCH_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                            "etlbatch", CURRENT_VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT = ArtifactSummary.from(BATCH_APP_ARTIFACT_ID);

  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private static HTable htable;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add artifact for batch sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      HBaseSource.class, HBaseSink.class, TableSink.class, StreamBatchSource.class,
                      TableInputFormat.class, TableOutputFormat.class,
                      Result.class, ImmutableBytesWritable.class,
                      Put.class, Mutation.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start HBase cluster
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    hBaseAdmin = testUtil.getHBaseAdmin();
    htable = testUtil.createTable(HBASE_TABLE_NAME.getBytes(), HBASE_FAMILY_COLUMN.getBytes());
    htable.put(new Put(ROW1.getBytes()).add(HBASE_FAMILY_COLUMN.getBytes(), COL1.getBytes(), VAL1.getBytes()));
    htable.put(new Put(ROW1.getBytes()).add(HBASE_FAMILY_COLUMN.getBytes(), COL2.getBytes(), VAL2.getBytes()));
    htable.put(new Put(ROW2.getBytes()).add(HBASE_FAMILY_COLUMN.getBytes(), COL1.getBytes(), VAL1.getBytes()));
    htable.put(new Put(ROW2.getBytes()).add(HBASE_FAMILY_COLUMN.getBytes(), COL2.getBytes(), VAL2.getBytes()));
  }

  @After
  public void afterTest() throws Exception {
    // Shutdown HBase
    htable.close();
    hBaseAdmin.close();
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testHBaseSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send("AAPL|10|500.32");
    streamManager.send("ORCL|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, STREAM_NAME)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    Map<String, String> hBaseProps = new HashMap<>();
    hBaseProps.put("tableName", HBASE_TABLE_NAME);
    hBaseProps.put("columnFamily", HBASE_FAMILY_COLUMN);
    hBaseProps.put("zkQuorum", "localhost");
    hBaseProps.put("zkClientPort", Integer.toString(testUtil.getZkCluster().getClientPort()));
    hBaseProps.put("schema", BODY_SCHEMA.toString());
    hBaseProps.put("zkNodeParent", testUtil.getConfiguration().get("zookeeper.znode.parent"));
    hBaseProps.put("rowField", "ticker");
    ETLStage sink = new ETLStage("HBase", hBaseProps);
    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "HBaseSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    ResultScanner resultScanner = htable.getScanner(HBASE_FAMILY_COLUMN.getBytes());
    Result result;
    int rowCount = 0;
    while (resultScanner.next() != null) {
      rowCount++;
    }
    resultScanner.close();
    Assert.assertEquals(4, rowCount);
    result = htable.get(new Get("ORCL".getBytes()));
    Assert.assertNotNull(result);
    Map<byte[], byte[]> orclData = result.getFamilyMap(HBASE_FAMILY_COLUMN.getBytes());
    Assert.assertEquals(2, orclData.size());
    Assert.assertEquals("13", Bytes.toString(orclData.get("col1".getBytes())));
    Assert.assertEquals("212.36", Bytes.toString(orclData.get("col2".getBytes())));
  }

  @Test
  public void testHBaseSource() throws Exception {
    Map<String, String> hBaseProps = new HashMap<>();
    hBaseProps.put("tableName", HBASE_TABLE_NAME);
    hBaseProps.put("columnFamily", HBASE_FAMILY_COLUMN);
    hBaseProps.put("zkQuorum", "localhost");
    hBaseProps.put("zkClientPort", Integer.toString(testUtil.getZkCluster().getClientPort()));
    hBaseProps.put("schema", BODY_SCHEMA.toString());
    hBaseProps.put("rowField", "ticker");

    ETLStage source = new ETLStage("HBase", hBaseProps);
    ETLStage sink = new ETLStage("Table", ImmutableMap.of("name", TABLE_NAME,
                                                          Table.PROPERTY_SCHEMA, BODY_SCHEMA.toString(),
                                                          Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker"));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "HBaseSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(TABLE_NAME);
    Table outputTable = outputManager.get();

    // Scanner to verify number of rows
    Scanner scanner = outputTable.scan(null, null);
    int countRows = 0;
    while (scanner.next() != null) {
      countRows++;
    }
    Assert.assertEquals(2, countRows);
    scanner.close();
    Row row = outputTable.get(ROW1.getBytes());
    Assert.assertNotNull(row);
    Assert.assertEquals(VAL1, Bytes.toString(row.get(COL1)));
    Assert.assertEquals(VAL2, Bytes.toString(row.get(COL2)));
    scanner.close();
    // Verify data
  }
}
