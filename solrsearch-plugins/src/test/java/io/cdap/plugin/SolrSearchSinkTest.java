/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.SolrSearchSink;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.SolrSearchSinkConfig;
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
import java.util.concurrent.TimeUnit;

/**
 * Test cases for {@link SolrSearchSink}.
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

  private static final String VERSION = "3.2.0";
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion(VERSION);
  private static final ArtifactId BATCH_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());
  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(NamespaceId.DEFAULT.getNamespace(),
                                                                              "data-pipeline",
                                                                              CURRENT_VERSION, true,
                                                                              CURRENT_VERSION, true);


  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private SolrClient client;

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(BATCH_ARTIFACT_RANGE);

    // add Solr search plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("solrsearch-plugins", "1.0.0"), parents,
                      SolrSearchSink.class, SolrSearchSinkConfig.class);
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
      .put("keyField", "id")
      .put("batchSize", "1000")
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
      .put("keyField", "id")
      .put("batchSize", "1000")
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
      .put("keyField", "id")
      .put("batchSize", "100")
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
      .put("keyField", "id")
      .put("batchSize", "1000")
      .put("outputFieldMappings", "office address:address")
      .build();

    ETLStage sink = new ETLStage("SolrSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE, sinkConfigproperties,
                                                           null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testBatchSolrSinkWrongHost");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, 1, 5, TimeUnit.MINUTES);
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
      .put("keyField", "id")
      .put("batchSize", "1000")
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("id", "1").set("firstname", "Brett").set("lastname", "Lee").set
        ("office address", "NE lake side").set("pincode", 480001).build(),
      StructuredRecord.builder(inputSchema).set("id", "2").set("firstname", "John").set("lastname", "Ray").set
        ("office address", "SE lake side").set("pincode", 480002).build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, 1, 5, TimeUnit.MINUTES);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSingleNodeSolrUrl() {
    SolrSearchSink.BatchSolrSearchConfig config = new SolrSearchSink.BatchSolrSearchConfig
      ("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE, "localhost:8983,localhost:8984", "collection1", "id",
       "office address:address", "1000");
    SolrSearchSink sinkObject = new SolrSearchSink(config);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    sinkObject.configurePipeline(configurer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongKeyFieldName() {
    SolrSearchSink.BatchSolrSearchConfig config = new SolrSearchSink.BatchSolrSearchConfig
      ("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE, "localhost:8983", "collection1", "wrong_id",
       "office address:address", "1000");
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

    SolrSearchSink.BatchSolrSearchConfig config = new SolrSearchSink.BatchSolrSearchConfig
      ("SolrSink", SolrSearchSinkConfig.SINGLE_NODE_MODE, "localhost:8983", "collection1", "id",
       "office address:address", "1000");
    SolrSearchSink sinkObject = new SolrSearchSink(config);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    sinkObject.configurePipeline(configurer);
  }
}
