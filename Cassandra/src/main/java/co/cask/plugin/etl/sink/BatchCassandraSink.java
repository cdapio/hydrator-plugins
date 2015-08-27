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

package co.cask.plugin.etl.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import com.google.common.base.Preconditions;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link BatchSink} that writes data to Cassandra.
 * This {@link BatchCassandraSink} takes a {@link StructuredRecord} in,
 * converts it to columns, and writes it to the Cassandra server.
 */
@Plugin(type = "sink")
@Name("Cassandra")
@Description("CDAP Cassandra Batch Sink takes the structured record from the input source " +
  "and converts each field to a byte buffer, then puts it in the keyspace and column family specified by the user. " +
  "The Cassandra server should be running prior to creating the adapter, " +
  "and the keyspace and column family should be created.")
public class BatchCassandraSink extends BatchSink<StructuredRecord, Map<String, ByteBuffer>, List<ByteBuffer>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchCassandraSink.class);
  private final CassandraBatchConfig config;

  public BatchCassandraSink(CassandraBatchConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();

    ConfigHelper.setOutputColumnFamily(conf, config.keyspace, config.columnFamily);
    ConfigHelper.setOutputInitialAddress(conf, config.initialAddress);
    ConfigHelper.setOutputPartitioner(conf, config.partitioner);
    ConfigHelper.setOutputRpcPort(conf, config.port);

    // The query needs to include the non-primary key columns. Creating the query
    // For example, the query might be "UPDATE keyspace.columnFamily SET column1 = ?, column2 = ? "
    // The primary keys are then added by Cassandra
    String query = String.format("UPDATE %s.%s SET ", config.keyspace, config.columnFamily);
    for (String column : config.columns.split(",")) {
      if (!Arrays.asList(config.primaryKey.split(",")).contains(column)) {
        query += column + " = ?, ";
      }
    }
    query = query.substring(0, query.lastIndexOf(",")) + " "; //to remove the last comma
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query);

    // ideally, we will use CqlBulkOutputFormat once Cassandra implements the patch
    // to make the Hadoop-CQL package compatible with Hadoop
    job.setOutputFormatClass(CqlOutputFormat.class);
  }

  @Override
  public void transform(StructuredRecord record,
                        Emitter<KeyValue<Map<String, ByteBuffer>, List<ByteBuffer>>> emitter) throws Exception {
    Map<String, ByteBuffer> keys = new LinkedHashMap<>();
    for (String key : config.primaryKey.split(",")) {
      Preconditions.checkNotNull(record.get(key), String.format("Primary key %s is not present in this record: %s",
                                                                key, StructuredRecordStringConverter
                                                                       .toDelimitedString(record, ";")));
      keys.put(key, encodeObject(record.get(key), record.getSchema().getField(key).getSchema()));
    }
    emitter.emit(new KeyValue<>(keys, getColumns(record)));
  }

  private List<ByteBuffer> getColumns(StructuredRecord record) throws Exception {
    List<ByteBuffer> columns = new ArrayList<>();
    List<String> primaryKeys = Arrays.asList(config.primaryKey.split(","));
    for (String columnName : config.columns.split(",")) {

      //Cassandra allows multiple primary keys, so splitting that list on a comma
      // and checking that the current column isn't a primary key
      if (!primaryKeys.contains(columnName)) {
        columns.add(encodeObject(record.get(columnName),
                                 record.getSchema().getField(columnName).getSchema()));
      }
    }
    return columns;
  }

  private ByteBuffer encodeObject(Object object, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      case BOOLEAN:
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((boolean) object ? 1 : 0);
        return ByteBuffer.wrap(bytes);
      case INT:
        return ByteBufferUtil.bytes((int) object);
      case LONG:
        return ByteBufferUtil.bytes((long) object);
      case FLOAT:
        return ByteBufferUtil.bytes((float) object);
      case DOUBLE:
        return ByteBufferUtil.bytes((double) object);
      case BYTES:
        return ByteBuffer.wrap((byte[]) object);
      case STRING:
      case ENUM:
        // Currently there is no standard container to represent enum type
        return ByteBufferUtil.bytes((String) object);
      case UNION:
        if (schema.isNullableSimple()) {
          return object == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : encodeObject(object, schema.getNonNullable());
        }
    }
    throw new IOException("Unsupported field type - only simple types are supported: " + schema);
  }

  /**
   * Config class for Batch Cassandra
   */
  public static class CassandraBatchConfig extends PluginConfig {
    @Name(Cassandra.PARTITIONER)
    @Description("The partitioner for the keyspace")
    private String partitioner;

    @Name(Cassandra.PORT)
    @Description("The rpc port for Cassandra. For example, 9160. " +
      "Please also check the configuration to make sure that start_rpc is true in cassandra.yaml")
    private String port;

    @Name(Cassandra.COLUMN_FAMILY)
    @Description("The column family to inject data into. Create the column family before starting the adapter.")
    private String columnFamily;

    @Name(Cassandra.KEYSPACE)
    @Description("The keyspace to inject data into. Create the keyspace before starting the adapter.")
    private String keyspace;

    @Name(Cassandra.INITIAL_ADDRESS)
    @Description("The initial address to connect to. For example, \"localhost\".")
    private String initialAddress;

    @Name(Cassandra.COLUMNS)
    @Description("A comma-separated list of columns in the column family. " +
      "The columns should be listed in the same order as they are stored in the column family.")
    private String columns;

    @Name(Cassandra.PRIMARY_KEY)
    @Description("A comma-separated list of primary keys. For example, \"key1,key2\"")
    private String primaryKey;

    public CassandraBatchConfig(String partitioner, String port, String columnFamily, String keyspace,
                                String initialAddress, String columns, String primaryKey) {
      this.partitioner = partitioner;
      this.initialAddress = initialAddress;
      this.port = port;
      this.columnFamily = columnFamily;
      this.keyspace = keyspace;
      this.columns = columns;
      this.primaryKey = primaryKey;
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
    public static final String COLUMNS = "columns";
    public static final String PRIMARY_KEY = "primaryKey";
  }
}
