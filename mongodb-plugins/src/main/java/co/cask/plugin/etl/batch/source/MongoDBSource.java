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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = "batchsource")
@Name("MongoDB")
public class MongoDBSource extends BatchSource<Object, BSONObject, StructuredRecord> {

  private static final Gson GSON = new Gson();
  private final MongoDBConfig config;

  public MongoDBSource(MongoDBConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
    MongoConfigUtil.setInputURI(conf, config.connectionString);
    if (!Strings.isNullOrEmpty(config.inputQuery)) {
      MongoConfigUtil.setQuery(conf, config.inputQuery);
    }
    job.setInputFormatClass(MongoConfigUtil.getInputFormat(conf));
  }

  @Override
  public void transform(KeyValue<Object, BSONObject> input, Emitter<StructuredRecord> emitter) throws Exception {
    BSONObject bsonObject = input.getValue();
    Schema schema = Schema.parseJson(config.schema);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), convertValue(bsonObject.get(field.getName()).toString(), field));
    }
    emitter.emit(builder.build());
  }

  private Object convertValue(String input, Schema.Field field) throws IOException {
    switch (field.getSchema().getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return Boolean.valueOf(input);
      case INT:
        return Integer.valueOf(input);
      case LONG:
        return Long.valueOf(input);
      case FLOAT:
        return Float.valueOf(input);
      case DOUBLE:
        return Double.valueOf(input);
      case BYTES:
        return input.getBytes();
      case STRING:
      case ENUM:
        return input;
      case ARRAY:
        return GSON.fromJson(input, List.class);
      case MAP:
        return GSON.fromJson(input, Map.class);
      case UNION:
        if (field.getSchema().isNullableSimple()) {
          return convertValue(input, Schema.Field.of(field.getName(), field.getSchema().getNonNullable()));
        } else {
          return null;
        }
    }
    throw new IOException(String.format("Unsupported schema : %s for field '%s'",
                                        field.getSchema(), field.getName()));
  }

  public static class MongoDBConfig extends PluginConfig {

    @Name(Properties.CONNECTION_STRING)
    private String connectionString;

    @Name(Properties.SCHEMA)
    private String schema;

    @Name(Properties.INPUT_QUERY)
    @Nullable
    private String inputQuery;
  }

  public static class Properties {
    public static final String CONNECTION_STRING = "connectionString";
    public static final String SCHEMA = "schema";
    public static final String INPUT_QUERY = "inputQuery";
  }
}
