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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.ReferenceRealtimeSink;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB Realtime Sink.
 */
@Plugin(type = "realtimesink")
@Name("MongoDB")
@Description("CDAP MongoDB Realtime Sink takes StructuredRecord from the previous stage and converts it to " +
  "BSONDocument and then writes to MongoDB")
public class MongoDBRealtimeSink extends ReferenceRealtimeSink<StructuredRecord> {
  private final MongoDBConfig config;
  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;

  public MongoDBRealtimeSink(MongoDBConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    MongoClientURI clientURI = new MongoClientURI(config.connectionString);
    mongoClient = new MongoClient(clientURI);
    mongoDatabase = mongoClient.getDatabase(config.dbName);
  }

  @Override
  public int write(Iterable<StructuredRecord> iterable, DataWriter dataWriter) throws Exception {
    int recordCount = 0;
    MongoCollection<Document> collection = mongoDatabase.getCollection(config.collectionName);
    List<Document> documentList = new ArrayList<>();
    for (StructuredRecord record : iterable) {
      Document document = new Document();
      for (Schema.Field field : record.getSchema().getFields()) {
        document.append(field.getName(), record.get(field.getName()));
      }
      documentList.add(document);
      recordCount++;
    }
    collection.insertMany(documentList);
    return recordCount;
  }

  /**
   * Config class for {@link MongoDBRealtimeSink}.
   */
  public static class MongoDBConfig extends ReferencePluginConfig {
    @Name(Properties.CONNECTION_STRING)
    @Description("MongoDB Connection String (see http://docs.mongodb.org/manual/reference/connection-string); " +
      "Example: 'mongodb://localhost:27017/analytics.users'.")
    private String connectionString;

    @Name(Properties.DB_NAME)
    @Description("MongoDB Database Name")
    private String dbName;

    @Name(Properties.COLLECTION_NAME)
    @Description("MongoDB Collection Name")
    private String collectionName;

    public MongoDBConfig(String referenceName, String connectionString, String dbName, String collectionName) {
      super(referenceName);
      this.connectionString = connectionString;
      this.dbName = dbName;
      this.collectionName = collectionName;
    }
  }

  /**
   * Property names for the config.
   */
  public static class Properties {
    public static final String CONNECTION_STRING = "connectionString";
    public static final String DB_NAME = "dbName";
    public static final String COLLECTION_NAME = "collectionName";
  }
}
