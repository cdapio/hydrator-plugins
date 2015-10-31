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

package co.cask.plugin.etl.test;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.batch.ETLMapReduce;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkerManager;
import co.cask.plugin.etl.batch.sink.MongoDBBatchSink;
import co.cask.plugin.etl.batch.source.MongoDBBatchSource;
import co.cask.plugin.etl.realtime.sink.MongoDBRealtimeSink;
import co.cask.plugin.etl.testclasses.StreamBatchSource;
import co.cask.plugin.etl.testclasses.TableSink;
import com.google.common.collect.ImmutableMap;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit Tests for {@link MongoDBBatchSource} and {@link MongoDBBatchSink}.
 */
public class MongoDBTest extends TestBase {
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");
  private static final Id.Artifact BATCH_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                            "etlbatch", CURRENT_VERSION);
  private static final Id.Artifact REALTIME_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                               "etlrealtime", "3.2.0");

  private static final ArtifactSummary ETLBATCH_ARTIFACT = ArtifactSummary.from(BATCH_APP_ARTIFACT_ID);
  private static final ArtifactSummary ETLREALTIME_ARTIFACT = ArtifactSummary.from(REALTIME_APP_ARTIFACT_ID);

  private static final String MONGO_DB = "cdap";
  private static final String MONGO_SOURCE_COLLECTIONS = "stocks";
  private static final String MONGO_SINK_COLLECTIONS = "copy";
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

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
    Schema.Field.of("agents", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

  private MongodForTestsFactory factory = null;
  private int mongoPort;

  @BeforeClass
  public static void setup() throws Exception {
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class, BatchSource.class.getPackage().getName(),
                   BatchSink.class.getPackage().getName(), PipelineConfigurable.class.getPackage().getName());

    //add the artifact for the etl realtime app
    addAppArtifact(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSink.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      MongoDBBatchSource.class, MongoInputFormat.class, MongoSplitter.class, MongoInputSplit.class,
                      TableSink.class,
                      MongoDBBatchSink.class, StreamBatchSource.class);

    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "realtime-sources", "1.0.0"), REALTIME_APP_ARTIFACT_ID,
                      DataGeneratorSource.class);
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "realtime-sinks", "1.0.0"), REALTIME_APP_ARTIFACT_ID,
                      MongoDBRealtimeSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start an embedded mongodb server
    factory = MongodForTestsFactory.with(Version.Main.V3_0);
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
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                                                                    DataGeneratorSource.TABLE_TYPE));
    ETLStage sink = new ETLStage("MongoDB", ImmutableMap.of(MongoDBRealtimeSink.Properties.CONNECTION_STRING,
                                                            String.format("mongodb://localhost:%d", mongoPort),
                                                            MongoDBRealtimeSink.Properties.DB_NAME, "cdap",
                                                            MongoDBRealtimeSink.Properties.COLLECTION_NAME, "real"));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, new ArrayList<ETLStage>());
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoDBRealtimeSinkTest");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(ETLREALTIME_ARTIFACT, etlConfig);
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
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, STREAM_NAME)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, SINK_BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("MongoDB", new ImmutableMap.Builder<String, String>()
                                            .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
                                                 String.format("mongodb://localhost:%d/%s.%s",
                                                               mongoPort, MONGO_DB, MONGO_SINK_COLLECTIONS)).build());
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, new ArrayList<ETLStage>());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoSinkTest");
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
    ETLStage source = new ETLStage("MongoDB", new ImmutableMap.Builder<String, String>()
      .put(MongoDBBatchSource.Properties.CONNECTION_STRING,
           String.format("mongodb://localhost:%d/%s.%s",
                         mongoPort, MONGO_DB, MONGO_SOURCE_COLLECTIONS))
      .put(MongoDBBatchSource.Properties.SCHEMA, SOURCE_BODY_SCHEMA.toString())
      .put(MongoDBBatchSource.Properties.SPLITTER_CLASS,
           StandaloneMongoSplitter.class.getSimpleName()).build());

    ETLStage sink = new ETLStage("MongoDB", new ImmutableMap.Builder<String, String>()
      .put(MongoDBBatchSink.Properties.CONNECTION_STRING,
           String.format("mongodb://localhost:%d/%s.%s",
                         mongoPort, MONGO_DB, MONGO_SINK_COLLECTIONS)).build());
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, new ArrayList<ETLStage>());

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
    ETLStage source = new ETLStage("MongoDB", new ImmutableMap.Builder<String, String>()
                                              .put(MongoDBBatchSource.Properties.CONNECTION_STRING,
                                                   String.format("mongodb://localhost:%d/%s.%s",
                                                                 mongoPort, MONGO_DB, MONGO_SOURCE_COLLECTIONS))
                                              .put(MongoDBBatchSource.Properties.SCHEMA, SOURCE_BODY_SCHEMA.toString())
                                              .put(MongoDBBatchSource.Properties.SPLITTER_CLASS,
                                                   StandaloneMongoSplitter.class.getSimpleName()).build());
    ETLStage sink = new ETLStage("Table", ImmutableMap.of(Properties.Table.NAME, TABLE_NAME,
                                                          Properties.Table.PROPERTY_SCHEMA,
                                                          SINK_BODY_SCHEMA.toString(),
                                                          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, new ArrayList<ETLStage>());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MongoSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(TABLE_NAME);
    Table outputTable = outputManager.get();

    // Scanner to verify the data in the table
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Assert.assertNotNull(row1);
    scanner.close();
    Assert.assertArrayEquals("AAPL".getBytes(), row1.getRow());
    Assert.assertEquals(10, (int) row1.getInt("num"));
    Assert.assertEquals(23.23, row1.getDouble("price"), 0.00001);
    Row row2 = scanner.next();
    Assert.assertNotNull(row2);
    scanner.close();
    Assert.assertArrayEquals("ORCL".getBytes(), row2.getRow());
    Assert.assertEquals(12, (int) row2.getInt("num"));
    Assert.assertEquals(10.10, row2.getDouble("price"), 0.00001);
  }
}
