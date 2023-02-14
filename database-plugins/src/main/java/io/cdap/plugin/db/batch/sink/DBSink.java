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

package io.cdap.plugin.db.batch.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.DBManager;
import io.cdap.plugin.FieldCase;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.db.DBRecord;
import io.cdap.plugin.common.db.DBUtils;
import io.cdap.plugin.common.db.dbrecordwriter.ColumnType;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import io.cdap.plugin.db.common.DBBaseConfig;
import io.cdap.plugin.db.common.FQNGenerator;
import io.cdap.plugin.db.connector.DBConnector;
import io.cdap.plugin.db.connector.DBConnectorConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * Sink that can be configured to export data to a database table.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DBSink.NAME)
@Description("Writes records to a database table. Each record will be written to a row in the table.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = DBConnector.NAME)})
public class DBSink extends ReferenceBatchSink<StructuredRecord, DBRecord, NullWritable> {
  public static final String NAME = "Database";
  private static final Logger LOG = LoggerFactory.getLogger(DBSink.class);

  private final DBSinkConfig dbSinkConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;
  private List<ColumnType> columnTypes;
  private List<String> columns;

  public DBSink(DBSinkConfig dbSinkConfig) {
    super(new ReferencePluginConfig(dbSinkConfig.getReferenceName()));
    this.dbSinkConfig = dbSinkConfig;
    this.dbManager = new DBManager(dbSinkConfig.getConnection(), dbSinkConfig.getJdbcPluginType());
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sink", dbSinkConfig.jdbcPluginType, dbSinkConfig.getJdbcPluginName());
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    if (dbSinkConfig.containsMacro(DBConnectorConfig.JDBC_PLUGIN_NAME)) {
      dbManager.validateCredentials(collector);
    } else {
      dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId(), collector);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    collector.getOrThrowException();
    
    LOG.debug("tableName = {}; pluginType = {}; pluginName = {}; connectionString = {}; columns = {}; " +
                "transaction isolation level: {}",
              dbSinkConfig.tableName, dbSinkConfig.jdbcPluginType, dbSinkConfig.getJdbcPluginName(),
              dbSinkConfig.getConnectionString(), dbSinkConfig.columns, dbSinkConfig.transactionIsolationLevel);

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    // make sure that the table exists
    try {
      Preconditions.checkArgument(
        dbManager.tableExists(driverClass, dbSinkConfig.tableName),
        "Table %s does not exist. Please check that the 'tableName' property " +
          "has been set correctly, and that the connection string %s points to a valid database.",
        dbSinkConfig.tableName, dbSinkConfig.getConnectionString());
    } finally {
      DBUtils.cleanup(driverClass);
    }

    context.addOutput(Output.of(dbSinkConfig.getReferenceName(),
                                new DBOutputFormatProvider(dbSinkConfig, driverClass, context.getArguments())));

    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, schema,
                    schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
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
    List<ColumnType> columnTypes = new ArrayList<>(columns.size());
    dbManager.ensureJDBCDriverIsAvailable(driverClass);

    try (Connection connection = DriverManager.getConnection(dbSinkConfig.getConnectionString(),
                                                             dbSinkConfig.getAllConnectionArguments())) {
      try (Statement statement = connection.createStatement();
           // Run a query against the DB table that returns 0 records, but returns valid ResultSetMetadata
           // that can be used to construct DBRecord objects to sink to the database table.
           ResultSet rs = statement.executeQuery(String.format("SELECT %s FROM %s WHERE 1 = 0",
                                                               dbSinkConfig.columns, dbSinkConfig.tableName))
      ) {
        ResultSetMetaData resultSetMetadata = rs.getMetaData();
        columns = ImmutableList.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(dbSinkConfig.columns));
        columnTypes.addAll(getMatchedColumnTypeList(resultSetMetadata, columns));
      }
    }

    this.columnTypes = Collections.unmodifiableList(columnTypes);
  }

  /**
   * Compare columns from schema with columns in table and returns list of matched columns in {@link ColumnType} format.
   *
   * @param resultSetMetadata result set metadata from table.
   * @param columns           list of columns from schema.
   * @return list of matched columns.
   */
  private List<ColumnType> getMatchedColumnTypeList(ResultSetMetaData resultSetMetadata, List<String> columns)
          throws SQLException {
    List<ColumnType> columnTypes = new ArrayList<>(columns.size());
    FieldCase fieldCase = FieldCase.toFieldCase(dbSinkConfig.getColumnNameCase());
    // JDBC driver column indices start with 1
    for (int i = 0; i < resultSetMetadata.getColumnCount(); i++) {
      String name = resultSetMetadata.getColumnName(i + 1);
      if (fieldCase == FieldCase.LOWER) {
        name = name.toLowerCase();
      } else if (fieldCase == FieldCase.UPPER) {
        name = name.toUpperCase();
      }
      String columnTypeName = resultSetMetadata.getColumnTypeName(i + 1);
      int type = resultSetMetadata.getColumnType(i + 1);
      String schemaColumnName = columns.get(i);
      Preconditions.checkArgument(schemaColumnName.toLowerCase().equals(name.toLowerCase()),
              "Missing column '%s' in SQL table", schemaColumnName);
      columnTypes.add(new ColumnType(schemaColumnName, columnTypeName, type));
    }
    return columnTypes;
  }

  private void recordLineage(BatchSinkContext context, Schema tableSchema, List<String> fieldNames) {
    Asset asset = Asset.builder(dbSinkConfig.getReferenceName()).
      setFqn(FQNGenerator.constructFQN(dbSinkConfig.getConnectionString(), dbSinkConfig.getReferenceName())).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to Database.", fieldNames);
    }
  }

  /**
   * {@link PluginConfig} for {@link DBSink}
   */
  public static class DBSinkConfig extends DBBaseConfig {
    public static final String COLUMNS = "columns";
    public static final String TABLE_NAME = "tableName";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

    @Name(COLUMNS)
    @Description("Comma-separated list of columns in the specified table to export to.")
    @Macro
    public String columns;

    @Name(TABLE_NAME)
    @Description("Name of the database table to write to.")
    @Macro
    public String tableName;

    @Nullable
    @Name(TRANSACTION_ISOLATION_LEVEL)
    @Description("The transaction isolation level for queries run by this sink. " +
      "Defaults to TRANSACTION_SERIALIZABLE. See java.sql.Connection#setTransactionIsolation for more details. " +
      "The Phoenix jdbc driver will throw an exception if the Phoenix database does not have transactions enabled " +
      "and this setting is set to true. For drivers like that, this should be set to TRANSACTION_NONE.")
    @Macro
    public String transactionIsolationLevel;
  }

  private static class DBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    DBOutputFormatProvider(DBSinkConfig dbSinkConfig,
                           Class<? extends Driver> driverClass,
                           SettableArguments pipelineArguments) {
      this.conf = new HashMap<>();

      conf.put(ETLDBOutputFormat.AUTO_COMMIT_ENABLED, String.valueOf(dbSinkConfig.getEnableAutoCommit()));
      if (dbSinkConfig.transactionIsolationLevel != null) {
        conf.put(TransactionIsolationLevel.CONF_KEY, dbSinkConfig.transactionIsolationLevel);
      }
      if (dbSinkConfig.getConnectionArguments() != null) {
        conf.put(DBUtils.CONNECTION_ARGUMENTS, dbSinkConfig.getConnectionArguments());
      }
      conf.put(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClass.getName());
      conf.put(DBConfiguration.URL_PROPERTY, dbSinkConfig.getConnectionString());
      if (dbSinkConfig.getUser() != null) {
        conf.put(DBConfiguration.USERNAME_PROPERTY, dbSinkConfig.getUser());
      }
      if (dbSinkConfig.getPassword() != null) {
        conf.put(DBConfiguration.PASSWORD_PROPERTY, dbSinkConfig.getPassword());
      }
      conf.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, dbSinkConfig.tableName);
      conf.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, dbSinkConfig.columns);

      // Configure batch size for commit operations is specified.
      if (pipelineArguments.has(ETLDBOutputFormat.COMMIT_BATCH_SIZE)) {
        conf.put(ETLDBOutputFormat.COMMIT_BATCH_SIZE, pipelineArguments.get(ETLDBOutputFormat.COMMIT_BATCH_SIZE));
      }
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
