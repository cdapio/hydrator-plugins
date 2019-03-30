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

package co.cask.hydrator.plugin.db.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.cdap.etl.api.validation.InvalidStageException;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.plugin.ConnectionConfig;
import co.cask.hydrator.plugin.DBConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.DriverCleanup;
import co.cask.hydrator.plugin.FieldCase;
import co.cask.hydrator.plugin.StructuredRecordUtils;
import co.cask.hydrator.plugin.db.batch.TransactionIsolationLevel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Batch source to read from a DB table
 */
@Plugin(type = "batchsource")
@Name("Database")
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class DBSource extends ReferenceBatchSource<LongWritable, DBRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  private final DBSourceConfig sourceConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;

  public DBSource(DBSourceConfig sourceConfig) {
    super(new ReferencePluginConfig(sourceConfig.referenceName));
    this.sourceConfig = sourceConfig;
    this.dbManager = new DBManager(sourceConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    sourceConfig.validate();
    Class<? extends Driver> driverClass = dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());

    Schema configuredSchema = sourceConfig.getSchema();
    if (configuredSchema != null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(sourceConfig.getSchema());
    } else if (sourceConfig.query != null) {
      try {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(getSchema(driverClass));
      } catch (IllegalAccessException | InstantiationException e) {
        throw new InvalidStageException("Unable to instantiate JDBC driver: " + e.getMessage(), e);
      } catch (SQLException e) {
        throw new IllegalArgumentException("SQL error while getting query schema: " + e.getMessage(), e);
      }
    }
  }

  private Schema getSchema(Class<? extends Driver> driverClass)
    throws IllegalAccessException, SQLException, InstantiationException {
    DriverCleanup driverCleanup = loadPluginClassAndGetDriver(driverClass);
    try (Connection connection = getConnection()) {
      String query = sourceConfig.importQuery;
      Statement statement = connection.createStatement();
      statement.setMaxRows(1);
      LOG.error("query = {}", query);
      if (query.contains("$CONDITIONS")) {
        query = removeConditionsClause(query);
      }
      LOG.error("query = {}", query);
      ResultSet resultSet = statement.executeQuery(query);
      return Schema.recordOf("outputSchema", DBUtils.getSchemaFields(resultSet));
    } finally {
      driverCleanup.destroy();
    }
  }

  private static String removeConditionsClause(String importQuerySring) {
    importQuerySring = importQuerySring.replaceAll("\\s{2,}", " ").toUpperCase();
    if (importQuerySring.contains("WHERE $CONDITIONS AND")) {
      importQuerySring = importQuerySring.replace("$CONDITIONS AND", "");
    } else if (importQuerySring.contains("WHERE $CONDITIONS")) {
      importQuerySring = importQuerySring.replace("WHERE $CONDITIONS", "");
    } else if (importQuerySring.contains("AND $CONDITIONS")) {
      importQuerySring = importQuerySring.replace("AND $CONDITIONS", "");
    }
    return importQuerySring;
  }

  private DriverCleanup loadPluginClassAndGetDriver(Class<? extends Driver> driverClass)
    throws IllegalAccessException, InstantiationException, SQLException {

    if (driverClass == null) {
      throw new InstantiationException(
        String.format("Unable to load Driver class with plugin type %s and plugin name %s",
                      sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName));
    }

    try {
      return DBUtils.ensureJDBCDriverIsAvailable(driverClass, sourceConfig.connectionString,
                                                 sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register driver {}", driverClass, e);
      throw e;
    }
  }

  private Connection getConnection() throws SQLException {
    Properties properties =
      ConnectionConfig.getConnectionArguments(sourceConfig.connectionArguments,
                                              sourceConfig.user,
                                              sourceConfig.password);
    return DriverManager.getConnection(sourceConfig.connectionString, properties);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    sourceConfig.validate();

    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "boundingQuery = {}; transaction isolation level: {}",
              sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName,
              sourceConfig.connectionString, sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery());
    Configuration hConf = new Configuration();
    hConf.clear();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (sourceConfig.user == null && sourceConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString,
                                  sourceConfig.user, sourceConfig.password);
    }
    DataDrivenETLDBInputFormat.setInput(hConf, DBRecord.class,
                                        sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
                                        sourceConfig.getEnableAutoCommit());
    if (sourceConfig.transactionIsolationLevel != null) {
      hConf.set(TransactionIsolationLevel.CONF_KEY, sourceConfig.transactionIsolationLevel);
    }
    if (sourceConfig.connectionArguments != null) {
      hConf.set(DBUtils.CONNECTION_ARGUMENTS, sourceConfig.connectionArguments);
    }
    if (sourceConfig.numSplits == null || sourceConfig.numSplits != 1) {
      if (!sourceConfig.getImportQuery().contains("$CONDITIONS")) {
        throw new IllegalArgumentException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
                                                         sourceConfig.importQuery));
      }
      hConf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, sourceConfig.splitBy);
    }
    if (sourceConfig.numSplits != null) {
      hConf.setInt(MRJobConfig.NUM_MAPS, sourceConfig.numSplits);
    }
    if (sourceConfig.schema != null) {
      hConf.set(DBUtils.OVERRIDE_SCHEMA, sourceConfig.schema);
    }
    LineageRecorder lineageRecorder = new LineageRecorder(context, sourceConfig.referenceName);
    lineageRecorder.createExternalDataset(sourceConfig.getSchema());
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, hConf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecordUtils.convertCase(
      input.getValue().getRecord(), FieldCase.toFieldCase(sourceConfig.columnNameCase)));
  }

  @Override
  public void destroy() {
    try {
      DBUtils.cleanup(driverClass);
    } finally {
      dbManager.destroy();
    }
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName);
  }

  /**
   * {@link PluginConfig} for {@link DBSource}
   */
  public static class DBSourceConfig extends DBConfig {
    public static final String IMPORT_QUERY = "importQuery";
    public static final String BOUNDING_QUERY = "boundingQuery";
    public static final String SPLIT_BY = "splitBy";
    public static final String NUM_SPLITS = "numSplits";
    public static final String SCHEMA = "schema";
    public static final String TRANSACTION_ISOLATION_LEVEL = "transactionIsolationLevel";

    // this is a hidden property, only used to fetch schema
    @Nullable
    String query;

    // only nullable for get schema button
    @Nullable
    @Name(IMPORT_QUERY)
    @Description("The SELECT query to use to import data from the specified table. " +
      "You can specify an arbitrary number of columns to import, or import all columns using *. " +
      "The Query should contain the '$CONDITIONS' string unless numSplits is set to one. " +
      "For example, 'SELECT * FROM table WHERE $CONDITIONS'. The '$CONDITIONS' string" +
      "will be replaced by 'splitBy' field limits specified by the bounding query.")
    @Macro
    String importQuery;

    @Nullable
    @Name(BOUNDING_QUERY)
    @Description("Bounding Query should return the min and max of the " +
      "values of the 'splitBy' field. For example, 'SELECT MIN(id),MAX(id) FROM table'. " +
      "This is required unless numSplits is set to one.")
    @Macro
    String boundingQuery;

    @Nullable
    @Name(SPLIT_BY)
    @Description("Field Name which will be used to generate splits. This is required unless numSplits is set to one.")
    @Macro
    String splitBy;

    @Nullable
    @Name(NUM_SPLITS)
    @Description("The number of splits to generate. If set to one, the boundingQuery is not needed, " +
      "and no $CONDITIONS string needs to be specified in the importQuery. If not specified, the " +
      "execution framework will pick a value.")
    @Macro
    Integer numSplits;

    @Nullable
    @Name(TRANSACTION_ISOLATION_LEVEL)
    @Description("The transaction isolation level for queries run by this sink. " +
      "Defaults to TRANSACTION_SERIALIZABLE. See java.sql.Connection#setTransactionIsolation for more details. " +
      "The Phoenix jdbc driver will throw an exception if the Phoenix database does not have transactions enabled " +
      "and this setting is set to true. For drivers like that, this should be set to TRANSACTION_NONE.")
    @Macro
    public String transactionIsolationLevel;

    @Nullable
    @Name(SCHEMA)
    @Description("The schema of records output by the source. This will be used in place of whatever schema comes " +
      "back from the query. This should only be used if there is a bug in your jdbc driver. For example, if a column " +
      "is not correctly getting marked as nullable.")
    String schema;

    private String getImportQuery() {
      return cleanQuery(importQuery);
    }

    private String getBoundingQuery() {
      return cleanQuery(boundingQuery);
    }

    private void validate() {
      boolean hasOneSplit = false;
      if (!containsMacro("numSplits") && numSplits != null) {
        if (numSplits < 1) {
          throw new InvalidConfigPropertyException(
            "Invalid value for numSplits. Must be at least 1, but got " + numSplits, "numSplits");
        }
        if (numSplits == 1) {
          hasOneSplit = true;
        }
      }

      if (!containsMacro("transactionIsolationLevel") && transactionIsolationLevel != null) {
        TransactionIsolationLevel.validate(transactionIsolationLevel);
      }

      if (query != null) {
        return;
      }

      if (!containsMacro("importQuery") && (query == null || query.isEmpty())
        && (importQuery == null || importQuery.isEmpty())) {
        throw new InvalidConfigPropertyException("An Import Query must be specified.", "importQuery");
      }

      if (!hasOneSplit && !containsMacro("importQuery") && !getImportQuery().contains("$CONDITIONS")) {
        throw new InvalidConfigPropertyException(String.format("Import Query %s must contain the string '$CONDITIONS'.",
                                                               importQuery), "importQuery");
      }

      if (!hasOneSplit && !containsMacro("splitBy") && (splitBy == null || splitBy.isEmpty())) {
        throw new InvalidConfigPropertyException("The splitBy property must be specified if numSplits is not set to 1.",
                                                 "splitBy");
      }

      if (!hasOneSplit && !containsMacro("boundingQuery") && (boundingQuery == null || boundingQuery.isEmpty())) {
        throw new InvalidConfigPropertyException("The boundingQuery must be specified if numSplits is not set to 1.",
                                                 "boundingQuery");
      }

    }

    @Nullable
    private Schema getSchema() {
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException(String.format("Unable to parse schema: %s", e.getMessage()), e,
                                                 "schema");
      }
    }
  }
}
