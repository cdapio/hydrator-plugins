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

package co.cask.hydrator.plugin.db.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
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
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.DBConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.FieldCase;
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
import java.util.TreeMap;


/**
 * Sink that can be configured to export data to a database table.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Database")
@Description("Writes records to a database table. Each record will be written to a row in the table.")
public class DBSink extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSink.class);

  private final DBSinkConfig dbSinkConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;
  private int [] columnTypes;
  private List<String> columns;

  public DBSink(DBSinkConfig dbSinkConfig) {
    super(new ReferencePluginConfig(dbSinkConfig.referenceName));
    this.dbSinkConfig = dbSinkConfig;
    this.dbManager = new DBManager(dbSinkConfig);
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {}",
              dbSinkConfig.tableName, dbSinkConfig.jdbcPluginType, dbSinkConfig.jdbcPluginName,
              dbSinkConfig.connectionString, dbSinkConfig.columns);

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the table exists
    try {
      Preconditions.checkArgument(
        dbManager.tableExists(driverClass, dbSinkConfig.tableName),
        "Table %s does not exist. Please check that the 'tableName' property " +
          "has been set correctly, and that the connection string %s points to a valid database.",
        dbSinkConfig.tableName, dbSinkConfig.connectionString);
    } finally {
      DBUtils.cleanup(driverClass);
    }
    context.addOutput(Output.of(dbSinkConfig.referenceName, new DBOutputFormatProvider(dbSinkConfig, driverClass)));
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
    for (String column : columns) {
      Schema.Field field = input.getSchema().getField(column);
      Preconditions.checkNotNull(field, "Missing schema field for column '%s'", column);
      outputFields.add(field);
    }
    StructuredRecord.Builder output = StructuredRecord.builder(
      Schema.recordOf(input.getSchema().getRecordName(), outputFields));
    for (String column : columns) {
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
  void setColumns(List<String> columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

  private void setResultSetMetadata() throws Exception {
    Map<String, Integer> columnToType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    dbManager.ensureJDBCDriverIsAvailable(driverClass);

    Connection connection;
    if (dbSinkConfig.user == null) {
      connection = DriverManager.getConnection(dbSinkConfig.connectionString);
    } else {
      connection = DriverManager.getConnection(dbSinkConfig.connectionString, dbSinkConfig.user, dbSinkConfig.password);
    }

    try {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbSinkConfig.columns, dbSinkConfig.tableName))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        FieldCase fieldCase = FieldCase.toFieldCase(dbSinkConfig.columnNameCase);
        // JDBC driver column indices start with 1
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
          String name = resultSetMetadata.getColumnName(i + 1);
          int type = resultSetMetadata.getColumnType(i + 1);
          if (fieldCase == FieldCase.LOWER) {
            name = name.toLowerCase();
          } else if (fieldCase == FieldCase.UPPER) {
            name = name.toUpperCase();
          }
          columnToType.put(name, type);
        }
      }
    } finally {
      connection.close();
    }

    columns = ImmutableList.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(dbSinkConfig.columns));
    columnTypes = new int[columns.size()];
    for (int i = 0; i < columnTypes.length; i++) {
      String name = columns.get(i);
      Preconditions.checkArgument(columnToType.containsKey(name), "Missing column '%s' in SQL table", name);
      columnTypes[i] = columnToType.get(name);
    }
  }

  /**
   * {@link PluginConfig} for {@link DBSink}
   */
  public static class DBSinkConfig extends DBConfig {
    public static final String COLUMNS = "columns";
    public static final String TABLE_NAME = "tableName";

    @Name(COLUMNS)
    @Description("Comma-separated list of columns in the specified table to export to.")
    @Macro
    public String columns;

    @Name(TABLE_NAME)
    @Description("Name of the database table to write to.")
    @Macro
    public String tableName;

  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DBOutputFormatProvider(DBSinkConfig dbSinkConfig, Class<? extends Driver> driverClass) {
      this.conf = new HashMap<>();

      conf.put(ETLDBOutputFormat.AUTO_COMMIT_ENABLED, String.valueOf(dbSinkConfig.getEnableAutoCommit()));
      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
      conf.put(DBConfiguration.URL_PROPERTY, dbSinkConfig.connectionString);
      if (dbSinkConfig.user != null) {
        conf.put(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.user);
      }
      if (dbSinkConfig.password != null) {
        conf.put(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.password);
      }
      conf.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, dbSinkConfig.tableName);
      conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbSinkConfig.columns);
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
