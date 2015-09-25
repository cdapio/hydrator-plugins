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
import co.cask.cdap.test.TestBase;
import co.cask.plugin.etl.batch.source.MongoDBSource;
import co.cask.plugin.etl.testclasses.TableSink;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.splitter.MongoSplitter;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MongoDBTest extends TestBase {
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");
  private static final Id.Artifact BATCH_APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT,
                                                                            "etlbatch", CURRENT_VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT = ArtifactSummary.from(BATCH_APP_ARTIFACT_ID);
  private static final String MONGO_DB = "cdap";
  private static final String MONGO_COLLECTIONS = "stocks";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private MongodForTestsFactory factory = null;
  private int mongoPort;

  @BeforeClass
  public static void setupTest() throws Exception {
    addAppArtifact(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class, BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      MongoDBSource.class, MongoInputFormat.class, MongoSplitter.class, MongoInputSplit.class,
                      TableSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start an embedded mongodb server
    factory = MongodForTestsFactory.with(Version.Main.V3_1);
    MongoClient mongoClient = factory.newMongo();
    List<ServerAddress> serverAddressList = mongoClient.getAllAddress();
    mongoPort = serverAddressList.get(0).getPort();
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    mongoDatabase.createCollection(MONGO_COLLECTIONS);
    DB db = mongoClient.getDB(MONGO_DB);
    DBCollection dbCollection = db.getCollection(MONGO_COLLECTIONS);
    dbCollection.insert(new BasicDBObject(ImmutableMap.of("ticker", "AAPL", "num", 10, "price", 23.23)));
    dbCollection.insert(new BasicDBObject(ImmutableMap.of("ticker", "ORCL", "num", 12, "price", 10.10)));
  }

  @After
  public void afterTest() throws Exception {
    if (factory != null) {
      factory.shutdown();
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testMongoDBSource() throws Exception {
    ETLStage source = new ETLStage("MongoDB", new ImmutableMap.Builder<String, String>()
                                              .put(MongoDBSource.Properties.CONNECTION_STRING,
                                                   String.format("mongodb://localhost:%d/%s.%s",
                                                                 mongoPort, MONGO_DB, MONGO_COLLECTIONS))
                                              .put(MongoDBSource.Properties.SCHEMA, BODY_SCHEMA.toString()).build());
    ETLStage sink = new ETLStage("Table", ImmutableMap.of(Properties.Table.NAME, TABLE_NAME,
                                                          Properties.Table.PROPERTY_SCHEMA, BODY_SCHEMA.toString(),
                                                          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, new ArrayList<ETLStage>());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CassandraSourceTest");
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
