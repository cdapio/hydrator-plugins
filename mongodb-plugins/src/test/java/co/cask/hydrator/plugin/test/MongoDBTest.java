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
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.batch.sink.MongoDBBatchSink;
import co.cask.hydrator.plugin.batch.source.MongoDBBatchSource;
import co.cask.hydrator.plugin.realtime.sink.MongoDBRealtimeSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit Tests for {@link MongoDBBatchSource} and {@link MongoDBBatchSink}.
 */
public class MongoDBTest extends HydratorTestBase {
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
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);
    setupRealtimeArtifacts(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class);

    Set<ArtifactRange> parents = ImmutableSet.of(REALTIME_ARTIFACT_RANGE, BATCH_ARTIFACT_RANGE);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("mongo-plugins", "1.0.0"), parents,
                      MongoDBBatchSource.class, MongoInputFormat.class, MongoSplitter.class, MongoInputSplit.class,
                      MongoDBBatchSink.class,
                      MongoDBRealtimeSink.class);
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
    DB db = mongoClient.getDB(MONGO_DB);
    DBCollection dbCollection = db.getCollection(MONGO_SOURCE_COLLECTIONS);
    BasicDBList basicDBList = new BasicDBList();
    basicDBList.put(1, "a1");
    basicDBList.put(2, "a2");
    dbCollection.insert(new BasicDBObject(ImmutableMap.of("ticker", "AAPL", "num", 10, "price", 23.23,
                                                          "agents", basicDBList)));
    dbCollection.insert(new BasicDBObject(ImmutableMap.of("ticker", "ORCL", "num", 12, "price", 10.10,
                                                          "agents", basicDBList)));
  }

  @After
  public void afterTest() throws Exception {
    if (factory != null) {
      factory.shutdown();
    }
  }

  @Test
  public void testMongoDBRealtimeSink() throws Exception {
    Schema schema = Schema.recordOf("dummy", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    List<StructuredRecord> input = ImmutableList.of(StructuredRecord.builder(schema).set("x", 0).build());
    ETLStage source = new ETLStage("source", co.cask.cdap.etl.mock.realtime.MockSource.getPlugin(input));
    ETLStage sink = new ETLStage("MongoDB", new ETLPlugin(
      "MongoDB",
      RealtimeSink.PLUGIN_TYPE,
      ImmutableMap.of(MongoDBRealtimeSink.Properties.CONNECTION_STRING,
                      String.format("mongodb://localhost:%d", mongoPort),
                      MongoDBRealtimeSink.Properties.DB_NAME, "cdap",
                      MongoDBRealtimeSink.Properties.COLLECTION_NAME, "real",
                      Constants.Reference.REFERENCE_NAME, "MongoDBTest"),
      null));
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoDBRealtimeSinkTest");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.class.getSimpleName());
    workerManager.start();

    final MongoClient mongoClient = factory.newMongo();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        MongoDatabase mongoDatabase = mongoClient.getDatabase("cdap");
        MongoCollection<Document> documents = mongoDatabase.getCollection("real");
        return documents.count() > 0;
      }
    }, 30, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    workerManager.stop();
  }

  @Test
  public void testMongoDBSink() throws Exception {
    String inputDatasetName = "input-batchsinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    ETLStage sink = new ETLStage("MongoDB", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
             String.format("mongodb://localhost:%d/%s.%s",
                           mongoPort, MONGO_DB, MONGO_SINK_COLLECTIONS))
        .put(Constants.Reference.REFERENCE_NAME, "MongoTestDBSink").build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(SINK_BODY_SCHEMA).set("ticker", "AAPL").set("num", 10).set("price", 500.32).build(),
      StructuredRecord.builder(SINK_BODY_SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 212.36).build()
    );
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, inputRecords);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    MongoClient mongoClient = factory.newMongo();
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoCollection<Document> documents = mongoDatabase.getCollection(MONGO_SINK_COLLECTIONS);
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
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoToMongoTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

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
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

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
}
