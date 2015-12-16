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
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
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
import co.cask.hydrator.plugin.batch.ESProperties;
import co.cask.hydrator.plugin.batch.sink.BatchElasticsearchSink;
import co.cask.hydrator.plugin.batch.source.ElasticsearchSource;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.realtime.RealtimeElasticsearchSink;
import co.cask.hydrator.plugin.testclasses.StreamBatchSource;
import co.cask.hydrator.plugin.testclasses.TableSink;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *  Unit test for batch {@link BatchElasticsearchSink} and {@link ElasticsearchSource} classes.
 */
public class ETLESTest extends TestBase {
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

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
  private Client client;
  private Node node;
  private int httpPort;
  private int transportPort;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    //add the artifact for the etl realtime app
    addAppArtifact(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    Set<ArtifactRange> parents = ImmutableSet.of(REALTIME_ARTIFACT_RANGE, BATCH_ARTIFACT_RANGE);

    // add artifact for batch sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), parents,
                      BatchElasticsearchSink.class, ElasticsearchSource.class, RealtimeElasticsearchSink.class);

    // add artifact for realtime sources and sinks
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugins", "1.0.0"), parents,
                      DataGeneratorSource.class, StreamBatchSource.class, TableSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    httpPort = Networks.getRandomPort();
    transportPort = Networks.getRandomPort();
    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
      .put("path.data", tmpFolder.newFolder("data"))
      .put("cluster.name", "testcluster")
      .put("http.port", httpPort)
      .put("transport.tcp.port", transportPort);
    node = nodeBuilder().settings(elasticsearchSettings.build()).client(false).node();
    client = node.client();
  }



  @After
  public void afterTest() {
    try {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("batch")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    } finally {
      node.close();
    }
  }

  @Test
  public void testES() throws Exception {
    testBatchESSink();
    testESSource();
    testRealtimeESSink();
  }

  private void testBatchESSink() throws Exception {
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

    ETLStage sink = new ETLStage("Elasticsearch", new Plugin(
      "Elasticsearch",
      ImmutableMap.of(ESProperties.HOST,
                      InetAddress.getLocalHost().getHostName() + ":" + httpPort,
                      ESProperties.INDEX_NAME, "batch",
                      ESProperties.TYPE_NAME, "testing",
                      ESProperties.ID_FIELD, "ticker"
      )));
    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "esSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    SearchResponse searchResponse = client.prepareSearch("batch").execute().actionGet();
    Assert.assertEquals(2, searchResponse.getHits().getTotalHits());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
    Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
    Assert.assertEquals("batch", searchResponse.getHits().getAt(0).getIndex());
    Assert.assertEquals("testing", searchResponse.getHits().getAt(0).getType());
    Assert.assertEquals("AAPL", searchResponse.getHits().getAt(0).getId());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
    Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

    DeleteResponse response = client.prepareDelete("batch", "testing", "CDAP").execute().actionGet();
    Assert.assertTrue(response.isFound());
  }

  @SuppressWarnings("ConstantConditions")
  private void testESSource() throws Exception {
    ETLStage source = new ETLStage("Elasticsearch", new Plugin(
      "Elasticsearch",
      ImmutableMap.of(ESProperties.HOST,
                      InetAddress.getLocalHost().getHostName() + ":" + httpPort,
                      ESProperties.INDEX_NAME, "batch",
                      ESProperties.TYPE_NAME, "testing",
                      ESProperties.QUERY, "?q=*",
                      ESProperties.SCHEMA, BODY_SCHEMA.toString())));
    ETLStage sink = new ETLStage("Table", new Plugin(
      "Table",
      ImmutableMap.of("name", TABLE_NAME,
                      Properties.Table.PROPERTY_SCHEMA, BODY_SCHEMA.toString(),
                      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker")));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "esSourceTest");
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
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals(10, (int) row1.getInt("num"));
    Assert.assertEquals(500.32, row1.getDouble("price"), 0.000001);
    Assert.assertNull(row1.get("NOT_IMPORTED"));
  }

  private void testRealtimeESSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", new Plugin(
      "DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE)));
    try {
      ETLStage sink = new ETLStage("Elasticsearch", new Plugin(
        "Elasticsearch",
        ImmutableMap.of(ESProperties.TRANSPORT_ADDRESSES,
                        InetAddress.getLocalHost().getHostName() + ":" + transportPort,
                        ESProperties.CLUSTER, "testcluster",
                        ESProperties.INDEX_NAME, "realtime",
                        ESProperties.TYPE_NAME, "testing",
                        ESProperties.ID_FIELD, "name"
        )));
      List<ETLStage> transforms = new ArrayList<>();
      ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);


      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testRealtimeSink");
      AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
      ApplicationManager appManager = deployApplication(appId, appRequest);

      WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

      workerManager.start();
      Tasks.waitFor(1L, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          try {
            SearchResponse searchResponse = client.prepareSearch("realtime").execute().actionGet();
            return searchResponse.getHits().getTotalHits();
          } catch (Exception e) {
            //the index test won't exist until the run is finished
            return 0L;
          }
        }
      }, 15, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      workerManager.stop();

      SearchResponse searchResponse = client.prepareSearch("realtime").execute().actionGet();
      Map<String, Object> result = searchResponse.getHits().getAt(0).getSource();

      Assert.assertEquals(1, (int) result.get("id"));
      Assert.assertEquals("Bob", result.get("name"));
      Assert.assertEquals(3.4, (double) result.get("score"), 0.000001);
      Assert.assertEquals(false, result.get("graduated"));
      Assert.assertNotNull(result.get("time"));

      searchResponse = client.prepareSearch().setQuery(matchQuery("score", "3.4")).execute().actionGet();
      Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
      SearchHit hit = searchResponse.getHits().getAt(0);
      Assert.assertEquals("realtime", hit.getIndex());
      Assert.assertEquals("testing", hit.getType());
      Assert.assertEquals("Bob", hit.getId());
      searchResponse = client.prepareSearch().setQuery(matchQuery("name", "ABCD")).execute().actionGet();
      Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

      DeleteResponse response = client.prepareDelete("realtime", "testing", "Bob").execute().actionGet();
      Assert.assertTrue(response.isFound());
    } finally {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("realtime")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    }
  }
}
