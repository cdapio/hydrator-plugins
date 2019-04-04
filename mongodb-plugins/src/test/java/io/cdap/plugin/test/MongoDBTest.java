/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.StandaloneMongoSplitter;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
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
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.sink.MongoDBBatchSink;
import io.cdap.plugin.batch.source.MongoDBBatchSource;
import io.cdap.plugin.common.Constants;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit Tests for {@link MongoDBBatchSource} and {@link MongoDBBatchSink}.
 */
public class MongoDBTest extends HydratorTestBase {
  private static final String VERSION = "3.2.0";
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion(VERSION);

  private static final ArtifactId BATCH_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(NamespaceId.DEFAULT.getNamespace(),
                                                                              "data-pipeline",
                                                                              CURRENT_VERSION, true,
                                                                              CURRENT_VERSION, true);

  private static final String MONGO_DB = "cdap";
  private static final String MONGO_SOURCE_COLLECTIONS = "stocks";
  private static final String MONGO_SINK_COLLECTIONS = "copy";

  private static final Schema SINK_BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Schema SOURCE_BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("agents", Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))));

  private MongodForTestsFactory factory = null;
  private int mongoPort;

  @BeforeClass
  public static void setup() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(BATCH_ARTIFACT_RANGE);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("mongo-plugins", "1.0.0"), parents,
                      MongoDBBatchSource.class, MongoInputFormat.class, MongoSplitter.class, MongoInputSplit.class,
                      MongoDBBatchSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start an embedded mongodb server
    factory = MongodForTestsFactory.with(Version.Main.V3_1);
    MongoClient mongoClient = factory.newMongo();
    List<ServerAddress> serverAddressList = mongoClient.getAllAddress();
    mongoPort = serverAddressList.get(0).getPort();
    mongoClient.dropDatabase(MONGO_DB);
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoIterable<String> collections = mongoDatabase.listCollectionNames();
    Assert.assertFalse(collections.iterator().hasNext());
    mongoDatabase.createCollection(MONGO_SOURCE_COLLECTIONS);
    MongoDatabase db = mongoClient.getDatabase(MONGO_DB);
    MongoCollection dbCollection = db.getCollection(MONGO_SOURCE_COLLECTIONS, BasicDBObject.class);
    BasicDBList basicDBList = new BasicDBList();
    basicDBList.put(1, "a1");
    basicDBList.put(2, "a2");
    dbCollection.insertOne(new BasicDBObject(ImmutableMap.of("ticker", "AAPL", "num", 10, "price", 23.23,
                                                             "agents", basicDBList)));
    dbCollection.insertOne(new BasicDBObject(ImmutableMap.of("ticker", "ORCL", "num", 12, "price", 10.10,
                                                             "agents", basicDBList)));
  }

  @After
  public void afterTest() throws Exception {
    if (factory != null) {
      factory.shutdown();
    }
  }

  @Test
  public void testMongoDBSink() throws Exception {
    String inputDatasetName = "input-batchsinktest";
    String secondCollectionName = MONGO_SINK_COLLECTIONS + "second";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    ETLStage sink1 = new ETLStage("MongoDB1", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, MONGO_SINK_COLLECTIONS))
        .put(Constants.Reference.REFERENCE_NAME, "MongoTestDBSink1").build(),
      null));
    ETLStage sink2 = new ETLStage("MongoDB2", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, secondCollectionName))
        .put(Constants.Reference.REFERENCE_NAME, "MongoTestDBSink2").build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(SINK_BODY_SCHEMA).set("ticker", "AAPL").set("num", 10).set("price", 500.32).build(),
      StructuredRecord.builder(SINK_BODY_SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 212.36).build()
    );
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    verifyMongoSinkData(MONGO_SINK_COLLECTIONS);
    verifyMongoSinkData(secondCollectionName);
  }

  @Test
  public void testMongoToMongo() throws Exception {
    ETLStage source = new ETLStage("MongoDBSource", new ETLPlugin(
      "MongoDB",
      BatchSource.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSource.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, MONGO_SOURCE_COLLECTIONS))
        .put(MongoDBBatchSource.Properties.SCHEMA, SOURCE_BODY_SCHEMA.toString())
        .put(MongoDBBatchSource.Properties.SPLITTER_CLASS, StandaloneMongoSplitter.class.getSimpleName())
        .put(Constants.Reference.REFERENCE_NAME, "MongoMongoTest").build(),
      null));

    ETLStage sink = new ETLStage("MongoDBSink", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, MONGO_SINK_COLLECTIONS))
        .put(Constants.Reference.REFERENCE_NAME, "MongoToMongoTest").build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoToMongoTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    MongoClient mongoClient = factory.newMongo();
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoCollection<Document> documents = mongoDatabase.getCollection(MONGO_SINK_COLLECTIONS);
    Assert.assertEquals(2, documents.count());
    Iterable<Document> docs = documents.find(new BasicDBObject("ticker", "AAPL"));
    Assert.assertTrue(docs.iterator().hasNext());
    for (Document document : docs) {
      Assert.assertEquals(10, (int) document.getInteger("num"));
      Assert.assertEquals(23.23, document.getDouble("price"), 0.0001);
      Assert.assertNotNull(document.get("agents"));
    }

    docs = documents.find(new BasicDBObject("ticker", "ORCL"));
    Assert.assertTrue(docs.iterator().hasNext());
    for (Document document : docs) {
      Assert.assertEquals(12, (int) document.getInteger("num"));
      Assert.assertEquals(10.10, document.getDouble("price"), 0.0001);
      Assert.assertNotNull(document.get("agents"));
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testMongoDBSource() throws Exception {
    ETLStage source = new ETLStage("MongoDB", new ETLPlugin(
      "MongoDB",
      BatchSource.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSource.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, MONGO_SOURCE_COLLECTIONS))
        .put(MongoDBBatchSource.Properties.SCHEMA, SOURCE_BODY_SCHEMA.toString())
        .put(MongoDBBatchSource.Properties.SPLITTER_CLASS,
             StandaloneMongoSplitter.class.getSimpleName())
        .put(Constants.Reference.REFERENCE_NAME, "SimpleMongoTest").build(),
      null));
    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    String ticker = outputRecords.get(0).get("ticker");
    StructuredRecord row1 = "AAPL".equals(ticker) ? outputRecords.get(0) : outputRecords.get(1);
    StructuredRecord row2 = "AAPL".equals(ticker) ? outputRecords.get(1) : outputRecords.get(0);

    Assert.assertEquals("AAPL", row1.get("ticker"));
    Assert.assertEquals(10, (int) row1.get("num"));
    Assert.assertEquals(23.23, (double) row1.get("price"), 0.00001);
    Assert.assertEquals("ORCL", row2.get("ticker"));
    Assert.assertEquals(12, (int) row2.get("num"));
    Assert.assertEquals(10.10, (double) row2.get("price"), 0.00001);
  }

  private void verifyMongoSinkData(String collectionName) throws Exception {
    MongoClient mongoClient = factory.newMongo();
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoCollection<Document> documents = mongoDatabase.getCollection(collectionName);
    Assert.assertEquals(2, documents.count());
    Iterable<Document> docs = documents.find(new BasicDBObject("ticker", "AAPL"));
    Assert.assertTrue(docs.iterator().hasNext());
    for (Document document : docs) {
      Assert.assertEquals(10, (int) document.getInteger("num"));
      Assert.assertEquals(500.32, document.getDouble("price"), 0.0001);
    }

    docs = documents.find(new BasicDBObject("ticker", "CDAP"));
    Assert.assertTrue(docs.iterator().hasNext());
    for (Document document : docs) {
      Assert.assertEquals(13, (int) document.getInteger("num"));
      Assert.assertEquals(212.36, document.getDouble("price"), 0.0001);
    }
  }
}
