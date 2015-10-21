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

package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.plugin.etl.batch.DBManager;
import co.cask.plugin.etl.batch.DBRecord;
import co.cask.plugin.etl.batch.DBUtils;
import co.cask.plugin.etl.batch.ETLDBOutputFormat;
import co.cask.plugin.etl.batch.TeradataConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink that can be configured to export data to a Teradata table.
 */
@Plugin(type = "batchsink")
@Name("Teradata")
@Description("Writes records to a Teradata table. Each record will be written to a row in the table.")
public class TeradataSink extends BatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(TeradataSink.class);

  private final TeradataSinkConfig sinkConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;
  private int [] columnTypes;
  private List<String> columns;

  public TeradataSink(TeradataSinkConfig sinkConfig) {
    this.sinkConfig = sinkConfig;
    this.dbManager = new DBManager(sinkConfig);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", sinkConfig.jdbcPluginType, sinkConfig.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {}",
              sinkConfig.tableName, sinkConfig.jdbcPluginType, sinkConfig.jdbcPluginName,
              sinkConfig.connectionString, sinkConfig.columns);

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    context.addOutput(sinkConfig.tableName, new DBOutputFormatProvider(sinkConfig, driverClass));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
    setResultSetMetadata();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<DBRecord, NullWritable>> emitter) throws Exception {
    // Create StructuredRecord that only has the columns in this.columns
    List<Schema.Field> outputFields = new ArrayList<>();
    for (String column : getColumns()) {
      Schema.Field field = input.getSchema().getField(column);
      Preconditions.checkNotNull(field, "Missing schema field for column '%s'", column);
      outputFields.add(field);
    }
    StructuredRecord.Builder output = StructuredRecord.builder(
      Schema.recordOf(input.getSchema().getRecordName(), outputFields));
    for (String column : getColumns()) {
      output.set(column, input.get(column));
    }

    emitter.emit(new KeyValue<DBRecord, NullWritable>(new DBRecord(output.build(), columnTypes), null));
  }

  @Override
  public void destroy() {
    DBUtils.cleanup(driverClass);
    dbManager.destroy();
  }

  @VisibleForTesting
  List<String> getColumns() {
    return columns;
  }

  private void setResultSetMetadata() throws Exception {
    dbManager.ensureJDBCDriverIsAvailable(driverClass);
    Map<String, Integer> columnToType = new HashMap<>();
    Connection connection;
    if (sinkConfig.user == null) {
      connection = DriverManager.getConnection(sinkConfig.connectionString);
    } else {
      connection = DriverManager.getConnection(sinkConfig.connectionString, sinkConfig.user, sinkConfig.password);
    }

    try {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               sinkConfig.columns, sinkConfig.tableName))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        // JDBC driver column indices start with 1
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
          String name = resultSetMetadata.getColumnName(i + 1);
          int type = resultSetMetadata.getColumnType(i + 1);
          columnToType.put(name, type);
        }
      }
    } finally {
      connection.close();
    }

    columns = ImmutableList.copyOf(Splitter.on(",").split(sinkConfig.columns));
    columnTypes = new int[columns.size()];
    for (int i = 0; i < columnTypes.length; i++) {
      String name = columns.get(i);
      Preconditions.checkArgument(columnToType.containsKey(name), "Missing column '%s' in SQL table", name);
      columnTypes[i] = columnToType.get(name);
    }
  }

  /**
   * {@link PluginConfig} for {@link TeradataSink}
   */
  public static class TeradataSinkConfig extends TeradataConfig {
    @Description("Comma-separated list of columns in the specified table to export to.")
    String columns;
  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    public DBOutputFormatProvider(TeradataSinkConfig sinkConfig, Class<? extends Driver> driverClass) {
      this.conf = new HashMap<>();

      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
      conf.put(DBConfiguration.URL_PROPERTY, sinkConfig.connectionString);
      if (sinkConfig.user != null && sinkConfig.password != null) {
        conf.put(DBConfiguration.USERNAME_PROPERTY, sinkConfig.user);
        conf.put(DBConfiguration.PASSWORD_PROPERTY, sinkConfig.password);
      }
      conf.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, sinkConfig.tableName);
      conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, sinkConfig.columns);
    }

    @Override
    public String getOutputFormatClassName() {
      return ETLDBOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
