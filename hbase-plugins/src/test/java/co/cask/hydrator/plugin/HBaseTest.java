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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.sink.HBaseSink;
import co.cask.hydrator.plugin.source.HBaseSource;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit Tests for {@link HBaseSource} and {@link HBaseSink}
 */
public class HBaseTest extends HydratorTestBase {
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

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private static HTable htable;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);

    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      HBaseSource.class, HBaseSink.class,
                      TableInputFormat.class, TableOutputFormat.class,
                      Result.class, ImmutableBytesWritable.class,
                      Put.class, Mutation.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start HBase cluster
    testUtil = new HBaseTestingUtility();
    MiniHBaseCluster hBaseCluster = testUtil.startMiniCluster();
    hBaseCluster.waitForActiveAndReadyMaster();
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
    if (htable != null) {
      htable.close();
    }

    if (hBaseAdmin != null) {
      hBaseAdmin.disableTable(HBASE_TABLE_NAME);
      hBaseAdmin.deleteTable(HBASE_TABLE_NAME);
      hBaseAdmin.close();
    }

    if (testUtil != null) {
      testUtil.shutdownMiniCluster();
    }
  }

  @Test
  public void testHBaseSink() throws Exception {
    String inputDatasetName = "input-hbasesinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> hBaseProps = new HashMap<>();
    hBaseProps.put("tableName", HBASE_TABLE_NAME);
    hBaseProps.put("columnFamily", HBASE_FAMILY_COLUMN);
    hBaseProps.put("zkClientPort", Integer.toString(testUtil.getZkCluster().getClientPort()));
    hBaseProps.put("schema", BODY_SCHEMA.toString());
    hBaseProps.put("zkNodeParent", testUtil.getConfiguration().get("zookeeper.znode.parent"));
    hBaseProps.put("rowField", "ticker");
    hBaseProps.put(Constants.Reference.REFERENCE_NAME, "HBaseSinkTest");
    ETLStage sink = new ETLStage("HBase", new ETLPlugin("HBase", BatchSink.PLUGIN_TYPE, hBaseProps, null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "HBaseSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(BODY_SCHEMA).set("ticker", "AAPL").set("col1", "10").set("col2", "500.32").build(),
      StructuredRecord.builder(BODY_SCHEMA).set("ticker", "ORCL").set("col1", "13").set("col2", "212.36").build()
    );
    MockSource.writeInput(inputManager, input);

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
    hBaseProps.put("zkClientPort", Integer.toString(testUtil.getZkCluster().getClientPort()));
    hBaseProps.put("schema", BODY_SCHEMA.toString());
    hBaseProps.put("rowField", "ticker");
    hBaseProps.put(Constants.Reference.REFERENCE_NAME, "HBaseSourceTest");

    ETLStage source = new ETLStage("HBase", new ETLPlugin("HBase", BatchSource.PLUGIN_TYPE, hBaseProps, null));
    String outputDatasetName = "output-hbasesourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "HBaseSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    String rowkey = outputRecords.get(0).get("ticker");
    StructuredRecord row1 = ROW1.equals(rowkey) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = ROW1.equals(rowkey) ? outputRecords.get(1) : outputRecords.get(0);

    Assert.assertEquals(ROW1, row1.get("ticker"));
    Assert.assertEquals(VAL1, row1.get(COL1));
    Assert.assertEquals(VAL2, row1.get(COL2));
    Assert.assertEquals(ROW2, row2.get("ticker"));
    Assert.assertEquals(VAL1, row2.get(COL1));
    Assert.assertEquals(VAL2, row2.get(COL2));
  }
}
