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

package io.cdap.plugin.batch.sink;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A {@link BatchSink} that writes data to MongoDB.
 * This {@link MongoDBBatchSink} takes a {@link StructuredRecord} in,
 * converts it to {@link BSONWritable}, and writes it to MongoDB.
 */
@Plugin(type = "batchsink")
@Name("MongoDB")
@Description("MongoDB Batch Sink converts a StructuredRecord to a BSONWritable and writes it to MongoDB.")
public class MongoDBBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, BSONWritable> {

  private final MongoDBSinkConfig config;

  public MongoDBBatchSink(MongoDBSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Configuration conf = new Configuration();
    String path = conf.get(
      "mapreduce.task.tmp.dir",
      conf.get(
        "mapred.child.tmp",
        conf.get("hadoop.tmp.dir", System.getProperty("java.io.tmpdir")))) + "/" + UUID.randomUUID().toString();
    context.addOutput(Output.of(config.referenceName, new MongoDBOutputFormatProvider(config, path)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, BSONWritable>> emitter)
    throws Exception {
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    for (Schema.Field field : input.getSchema().getFields()) {
      bsonBuilder.add(field.getName(), input.get(field.getName()));
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new BSONWritable(bsonBuilder.get())));
  }

  private static class MongoDBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    MongoDBOutputFormatProvider(MongoDBSinkConfig config, String path) {
      this.conf = new HashMap<>();
      conf.put("mongo.output.uri", config.connectionString);
      conf.put("mapreduce.task.tmp.dir", path);
    }

    @Override
    public String getOutputFormatClassName() {
      return MongoOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config class for {@link MongoDBBatchSink}
   */
  public static class MongoDBSinkConfig extends ReferencePluginConfig {
    @Name(Properties.CONNECTION_STRING)
    @Description("MongoDB Connection String (see http://docs.mongodb.org/manual/reference/connection-string); " +
      "Example: 'mongodb://localhost:27017/analytics.users'.")
    @Macro
    private String connectionString;

    public MongoDBSinkConfig(String referenceName, String connectionString) {
      super(referenceName);
      this.connectionString = connectionString;
    }
  }

  /**
   * Property names for config
   */
  public static class Properties {
    public static final String CONNECTION_STRING = "connectionString";
  }
}
