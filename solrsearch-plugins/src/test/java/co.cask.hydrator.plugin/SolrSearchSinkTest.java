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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.MockPipelineConfigurer;
import co.cask.hydrator.plugin.batch.SolrSearchSink;
import co.cask.hydrator.plugin.common.SolrSearchSinkConfig;
import co.cask.hydrator.plugin.realtime.RealtimeSolrSearchSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Test cases for {@Link SolrSearchSink} and {@Link RealtimeSolrSearchSink} classes.
 */
public class SolrSearchSinkTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final Schema inputSchema = Schema.recordOf(
    "input-record",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("firstname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("lastname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("office address", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("pincode", Schema.of(Schema.Type.INT)));

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


  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private SolrClient client;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);

    setupRealtimeArtifacts(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class);

    Set<ArtifactRange> parents = ImmutableSet.of(BATCH_ARTIFACT_RANGE, REALTIME_ARTIFACT_RANGE);

    // add Solr search plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("solrsearch-plugins", "1.0.0"), parents,
                      SolrSearchSink.class, SolrSearchSinkConfig.class, RealtimeSolrSearchSink.class);
  }

  @Ignore
  public void testBatchSolrSearchSink() throws Exception {
    client = new HttpSolrClient("http://localhost:8983/solr/collection1");
    String inputDatasetName = "solr-batch-input-source";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SolrSearchSinkConfig.SINGLE_NODE_MODE)
      .put("solrHost", "localhost:8983")
      .put("collectionName", "collection1")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrSink");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(2, resultList.size());
    for (SolrDocument document : resultList) {
      if (document.get("id").equals("1")) {
        Assert.assertEquals("Brett", document.get("firstname"));
        Assert.assertEquals("Lee", document.get("lastname"));
        Assert.assertEquals("NE lake side", document.get("address"));
        Assert.assertEquals(480001, document.get("pincode"));
      } else {
        Assert.assertEquals("John", document.get("firstname"));
        Assert.assertEquals("Ray", document.get("lastname"));
        Assert.assertEquals("SE lake side", document.get("address"));
        Assert.assertEquals(480002, document.get("pincode"));
      }
    }
    // Clean the indexes
    client.deleteByQuery("*:*");
    client.commit();
    client.shutdown();
  }

  @Ignore
  public void testRealTimeSolrSearchSink() throws Exception {
    client = new HttpSolrClient("http://localhost:8983/solr/collection1");

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "3").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480003).build(),
      StructuredRecord.builder(inputSchema).set("id", "4").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480004).build()
    );
    ETLStage source = new ETLStage("RealtimeSolrSource", co.cask.cdap.etl.mock.realtime.MockSource.getPlugin(input));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "RealtimeSolrSink")
      .put("solrMode", SolrSearchSinkConfig.SINGLE_NODE_MODE)
      .put("solrHost", "localhost:8983")
      .put("collectionName", "collection1")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", RealtimeSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testRealTimeSolrSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
        return queryResponse.getResults().size() > 0;
      }
    }, 30, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    workerManager.stop();

    QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(2, resultList.size());
    for (SolrDocument document : resultList) {
      if (document.get("id").equals("3")) {
        Assert.assertEquals("Brett", document.get("firstname"));
        Assert.assertEquals("Lee", document.get("lastname"));
        Assert.assertEquals("NE lake side", document.get("address"));
        Assert.assertEquals(480003, document.get("pincode"));
      } else {
        Assert.assertEquals("John", document.get("firstname"));
        Assert.assertEquals("Ray", document.get("lastname"));
        Assert.assertEquals("SE lake side", document.get("address"));
        Assert.assertEquals(480004, document.get("pincode"));
      }
    }
    // Clean the indexes
    client.deleteByQuery("*:*");
    client.commit();
    client.shutdown();
  }

  @Ignore
  public void testSolrCloudModeSink() throws Exception {
    CloudSolrClient cloudClient = new CloudSolrClient("localhost:2181");
    cloudClient.setDefaultCollection("collection1");

    String inputDatasetName = "solrcloud-batch-input-source";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "SolrCloudSink")
      .put("solrMode", SolrSearchSinkConfig.SOLR_CLOUD_MODE)
      .put("solrHost", "localhost:2181")
      .put("collectionName", "collection1")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrCloudSink");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    QueryResponse queryResponse = cloudClient.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(2, resultList.size());
    for (SolrDocument document : resultList) {
      if (document.get("id").equals("1")) {
        Assert.assertEquals("Brett", document.get("firstname"));
        Assert.assertEquals("Lee", document.get("lastname"));
        Assert.assertEquals("NE lake side", document.get("address"));
        Assert.assertEquals(480001, document.get("pincode"));
      } else {
        Assert.assertEquals("John", document.get("firstname"));
        Assert.assertEquals("Ray", document.get("lastname"));
        Assert.assertEquals("SE lake side", document.get("address"));
        Assert.assertEquals(480002, document.get("pincode"));
      }
    }
    // Clean the indexes
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    cloudClient.shutdown();
  }

  @Ignore
  public void testSolrSinkWithNullValues() throws Exception {
    Schema nullInputSchema = Schema.recordOf(
      "input-record",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("firstname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("lastname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("office address", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("pincode", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    client = new HttpSolrClient("http://localhost:8983/solr/collection1");
    String inputDatasetName = "input-source-with-null";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SolrSearchSinkConfig.SINGLE_NODE_MODE)
      .put("solrHost", "localhost:8983")
      .put("collectionName", "collection1")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrSink");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(nullInputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(nullInputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", null).build(),
      StructuredRecord.builder(nullInputSchema).set("id", "3").set("firstname", "Johnny").set("lastname", "Wagh").set
        ("office address", "").set("pincode", 480003).build(),
      StructuredRecord.builder(nullInputSchema).set("id", "").set("firstname", "Michael").set("lastname", "Hussey").set
        ("office address", "WE Lake Side").set("pincode", 480004).build(),
      StructuredRecord.builder(nullInputSchema).set("id", null).set("firstname", "Michael").set("lastname", "Clarke")
        .set("office address", "WE Lake Side").set("pincode", 480005).build());
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(4, resultList.size());
    for (SolrDocument document : resultList) {
      if (document.get("id").equals("1")) {
        Assert.assertEquals("Brett", document.get("firstname"));
        Assert.assertEquals("Lee", document.get("lastname"));
        Assert.assertEquals("NE lake side", document.get("address"));
        Assert.assertEquals(480001, document.get("pincode"));
      } else if (document.get("id").equals("2")) {
        Assert.assertEquals("John", document.get("firstname"));
        Assert.assertEquals("Ray", document.get("lastname"));
        Assert.assertEquals("SE lake side", document.get("address"));
      } else if (document.get("id").equals("3")) {
        Assert.assertEquals("Johnny", document.get("firstname"));
        Assert.assertEquals("Wagh", document.get("lastname"));
        Assert.assertEquals("", document.get("address"));
        Assert.assertEquals(480003, document.get("pincode"));
      } else {
        Assert.assertEquals("Michael", document.get("firstname"));
        Assert.assertEquals("Hussey", document.get("lastname"));
        Assert.assertEquals("WE Lake Side", document.get("address"));
        Assert.assertEquals(480004, document.get("pincode"));
      }
    }
    // Clean the indexes
    client.deleteByQuery("*:*");
    client.commit();
    client.shutdown();
  }

  @Test
  public void testSolrConnectionWithWrongHost() throws Exception {
    String inputDatasetName = "input-source-with-wrong-host";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SolrSearchSinkConfig.SINGLE_NODE_MODE)
      .put("solrHost", "localhost:8984")
      .put("collectionName", "collection1")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrSink");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());
  }

  @Test
  public void testSolrConnectionWithWrongCollection() throws Exception {
    String inputDatasetName = "input-source-with-wrong-collection";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> sinkConfigproperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SolrSearchSinkConfig.SINGLE_NODE_MODE)
      .put("solrHost", "localhost:8983")
      .put("collectionName", "wrong_collection")
      .put("idField", "id")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrSink");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSingleNodeSolrUrl() {
    SolrSearchSinkConfig config = new SolrSearchSinkConfig("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE,
                                                           "localhost:8983,localhost:8984", "collection1", "id",
                                                           "office address:address");
    SolrSearchSink sinkObject = new SolrSearchSink(config);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    sinkObject.configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongIdFieldName() {
    SolrSearchSinkConfig config = new SolrSearchSinkConfig("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE,
                                                           "localhost:8983", "collection1", "wrong_id",
                                                           "office address:address");
    SolrSearchSink sinkObject = new SolrSearchSink(config);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    sinkObject.configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInputDataType() {
    Schema inputSchema = Schema.recordOf(
      "input-record",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("firstname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("lastname", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("office address", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("pincode", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    SolrSearchSinkConfig config = new SolrSearchSinkConfig("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE,
                                                           "localhost:8983", "collection1", "id",
                                                           "office address:address");
    SolrSearchSink sinkObject = new SolrSearchSink(config);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    sinkObject.configurePipeline(configurer);
  }
}
