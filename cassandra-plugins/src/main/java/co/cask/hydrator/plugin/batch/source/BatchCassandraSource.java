/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import com.datastax.driver.core.Row;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source for Cassandra.
 * <p>
 * Note that one mapper will be created for each token. The default number of tokens is 256,
 * so a Map-Reduce job will run with 257 mappers, even for small datasets.
 * </p>
 */
// The issue of each token creating one mapper is documented in this Cassandra JIRA:
// https://issues.apache.org/jira/browse/CASSANDRA-6091

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Cassandra")
@Description("CDAP Cassandra Batch Source will select the rows returned by the user's query " +
  "and convert each row to a structured record using the schema specified by the user. ")
public class BatchCassandraSource extends ReferenceBatchSource<Long, Row, StructuredRecord> {
  private static final Map<Schema.Type, Class<?>> TYPE_CLASS_MAP = new ImmutableMap.Builder<Schema.Type, Class<?>>()
                                                                    .put(Schema.Type.BOOLEAN, boolean.class)
                                                                    .put(Schema.Type.BYTES, ByteBuffer.class)
                                                                    .put(Schema.Type.DOUBLE, double.class)
                                                                    .put(Schema.Type.FLOAT, float.class)
                                                                    .put(Schema.Type.INT, int.class)
                                                                    .put(Schema.Type.LONG, long.class)
                                                                    .put(Schema.Type.ENUM, String.class)
                                                                    .build();
  private final CassandraSourceConfig config;

  public BatchCassandraSource(CassandraSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.schema), "Schema must be specified.");
    try {
      Schema schema = Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration conf = new Configuration();
    conf.clear();

    ConfigHelper.setInputColumnFamily(conf, config.keyspace, config.columnFamily);
    ConfigHelper.setInputInitialAddress(conf, config.initialAddress);
    ConfigHelper.setInputPartitioner(conf, config.partitioner);
    ConfigHelper.setInputRpcPort(conf, (config.port == null) ? "9160" : Integer.toString(config.port));
    Preconditions.checkArgument(!(Strings.isNullOrEmpty(config.username) ^ Strings.isNullOrEmpty(config.password)),
                                "You must either set both username and password or neither username nor password. " +
                                  "Currently, they are username: " + config.username +
                                  " and password: " + config.password);
    if (!Strings.isNullOrEmpty(config.username)) {
      ConfigHelper.setInputKeyspaceUserNameAndPassword(conf, config.username, config.password);
    }

    if (!Strings.isNullOrEmpty(config.properties)) {
      for (String pair : config.properties.split(",")) {
        // the key and value of properties might have spaces so remove only leading and trailing ones
        conf.set(CharMatcher.WHITESPACE.trimFrom(pair.split(":")[0]),
                 CharMatcher.WHITESPACE.trimFrom(pair.split(":")[1]));
      }
    }
    CqlConfigHelper.setInputCql(conf, config.query);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CqlInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<Long, Row> input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema;
    try {
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), extractValue(input.getValue(), field));
    }
    emitter.emit(builder.build());
  }

  private Object extractValue(Row row, Schema.Field field) throws Exception {
    switch (field.getSchema().getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return row.getBool(field.getName());
      case INT:
        return row.getInt(field.getName());
      case LONG:
        return row.getLong(field.getName());
      case FLOAT:
        return row.getFloat(field.getName());
      case DOUBLE:
        return row.getDouble(field.getName());
      case BYTES:
        return row.getBytes(field.getName());
      case STRING:
      case ENUM:
        // Currently there is no standard container to represent enum type
        return row.getString(field.getName());
      case ARRAY:
        return row.getList(field.getName(), TYPE_CLASS_MAP.get(field.getSchema().getType()));
      case MAP:
        return row.getMap(field.getName(), TYPE_CLASS_MAP.get(field.getSchema().getMapSchema().getKey().getType()),
                          TYPE_CLASS_MAP.get(field.getSchema().getMapSchema().getValue().getType()));
      case UNION:
        if (field.getSchema().isNullableSimple()) {
          try {
            return extractValue(row, Schema.Field.of(field.getName(), field.getSchema().getNonNullable()));
          } catch (Exception e) {
            return null;
          }
        }
    }
    throw new IOException(String.format("Unsupported schema: %s for field: \'%s\'",
                                        field.getSchema(), field.getName()));
  }

  /**
   * Config class for Batch Cassandra Config
   */
  public static class CassandraSourceConfig extends ReferencePluginConfig {
    @Name(Cassandra.PARTITIONER)
    @Description("The partitioner for the keyspace")
    @Macro
    private String partitioner;

    @Name(Cassandra.PORT)
    @Nullable
    @Description("The RPC port for Cassandra; for example: 9160 (default value). " +
      "Check the configuration to make sure that start_rpc is true in cassandra.yaml.")
    @Macro
    private Integer port;

    @Name(Cassandra.COLUMN_FAMILY)
    @Description("The column family to select data from.")
    @Macro
    private String columnFamily;

    @Name(Cassandra.KEYSPACE)
    @Description("The keyspace to select data from.")
    @Macro
    private String keyspace;

    @Name(Cassandra.INITIAL_ADDRESS)
    @Description("The initial address to connect to. For example: \"10.11.12.13\".")
    @Macro
    private String initialAddress;

    @Name(Cassandra.USERNAME)
    @Description("The username for the keyspace (if one exists). " +
      "If this is not empty, then you must supply a password.")
    @Nullable
    @Macro
    private String username;

    @Name(Cassandra.PASSWORD)
    @Description("The password for the keyspace (if one exists). " +
      "If this is not empty, then you must supply a username.")
    @Nullable
    @Macro
    private String password;

    @Name(Cassandra.QUERY)
    @Description("The query to select data on. For example: \'SELECT * from table " +
      "where token(id) > ? and token(id) <= ?\'")
    @Macro
    private String query;

    @Name(Cassandra.SCHEMA)
    @Description("The schema for the data as it will be formatted in CDAP. Sample schema: {\n" +
      "    \"type\": \"record\",\n" +
      "    \"name\": \"schemaBody\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\": \"name\",\n" +
      "            \"type\": \"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"age\",\n" +
      "            \"type\": \"int\"\n" +
      "        }" +
      "    ]\n" +
      "}")
    private String schema;

    @Name(Cassandra.PROPERTIES)
    @Description("Any extra properties to include. The property-value pairs should be comma-separated, " +
      "and each property should be separated by a colon from its corresponding value. " +
      "For example: \'cassandra.consistencylevel.read:LOCAL_ONE,cassandra.input.native.port:9042\'")
    @Nullable
    private String properties;

    public CassandraSourceConfig(String referenceName, String partitioner, Integer port, String columnFamily,
                                 String schema, String keyspace, String initialAddress, String query,
                                 @Nullable String properties, @Nullable String username, @Nullable String password) {
      super(referenceName);
      this.partitioner = partitioner;
      this.initialAddress = initialAddress;
      this.port = port;
      this.columnFamily = columnFamily;
      this.keyspace = keyspace;
      this.username = username;
      this.password = password;
      this.query = query;
      this.schema = schema;
      this.properties = properties;
    }
  }

  /**
   * Properties for Cassandra
   */
  public static class Cassandra {
    public static final String PARTITIONER = "partitioner";
    public static final String PORT = "port";
    public static final String COLUMN_FAMILY = "columnFamily";
    public static final String KEYSPACE = "keyspace";
    public static final String INITIAL_ADDRESS = "initialAddress";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String QUERY = "query";
    public static final String SCHEMA = "schema";
    public static final String PROPERTIES = "properties";
  }
}

