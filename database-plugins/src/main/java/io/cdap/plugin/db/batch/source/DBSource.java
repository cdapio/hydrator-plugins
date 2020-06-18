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

package io.cdap.plugin.db.batch.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.ConnectionConfig;
import io.cdap.plugin.DBConfig;
import io.cdap.plugin.DBManager;
import io.cdap.plugin.DBRecord;
import io.cdap.plugin.DBUtils;
import io.cdap.plugin.DriverCleanup;
import io.cdap.plugin.FieldCase;
import io.cdap.plugin.StructuredRecordUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
  private static final Pattern CONDITIONS_AND = Pattern.compile("\\$conditions (and|or)\\s+",
                                                                Pattern.CASE_INSENSITIVE);
  private static final Pattern AND_CONDITIONS = Pattern.compile("\\s+(and|or) \\$conditions",
                                                                Pattern.CASE_INSENSITIVE);
  private static final Pattern WHERE_CONDITIONS = Pattern.compile("\\s+where \\$conditions",
                                                                  Pattern.CASE_INSENSITIVE);

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
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    sourceConfig.validate(collector);
    Class<? extends Driver> driverClass = dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId(),
                                                                               collector);
    // throw exception before deriving schema from database. This is because database schema is derived using import
    // query and its possible that validation failed for import query.
    collector.getOrThrowException();

    Schema configuredSchema = sourceConfig.getSchema(collector);
    if (configuredSchema != null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
    } else if (!sourceConfig.containsMacro(DBSourceConfig.IMPORT_QUERY)) {
      try {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(getSchema(driverClass, sourceConfig.patternToReplace,
                                                                          sourceConfig.replaceWith));
      } catch (IllegalAccessException | InstantiationException e) {
        collector.addFailure(String.format("Failed to instantiate JDBC driver: %s", e.getMessage()), null);
      } catch (SQLException e) {
        collector.addFailure(
          String.format("Encountered SQL error while getting query schema: %s", e.getMessage()), null);
      }
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    sourceConfig.validate(collector);
    collector.getOrThrowException();

    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "boundingQuery = {}; transaction isolation level: {}",
              sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName,
              sourceConfig.connectionString, sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
              sourceConfig.transactionIsolationLevel);
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
    if (sourceConfig.patternToReplace != null) {
      hConf.set(DBUtils.PATTERN_TO_REPLACE, sourceConfig.patternToReplace);
    }
    if (sourceConfig.replaceWith != null) {
      hConf.set(DBUtils.REPLACE_WITH, sourceConfig.replaceWith);
    }
    if (sourceConfig.fetchSize != null) {
      hConf.setInt(DBUtils.FETCH_SIZE, sourceConfig.fetchSize);
    }
    context.setInput(Input.of(sourceConfig.referenceName,
                              new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, hConf)));

    emitLineage(context);
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

  private Schema getSchema(Class<? extends Driver> driverClass, @Nullable String patternToReplace,
                           @Nullable String replaceWith)
    throws IllegalAccessException, SQLException, InstantiationException {
    DriverCleanup driverCleanup = loadPluginClassAndGetDriver(driverClass);
    try (Connection connection = getConnection()) {
      String query = sourceConfig.importQuery;
      Statement statement = connection.createStatement();
      statement.setMaxRows(1);
      if (query.contains("$CONDITIONS")) {
        query = removeConditionsClause(query);
      }
      ResultSet resultSet = statement.executeQuery(query);
      return Schema.recordOf("outputSchema", DBUtils.getSchemaFields(resultSet, patternToReplace, replaceWith));
    } finally {
      driverCleanup.destroy();
    }
  }

  @VisibleForTesting
  static String removeConditionsClause(String importQueryString) {
    String query = importQueryString;
    query = CONDITIONS_AND.matcher(query).replaceAll("");
    query = AND_CONDITIONS.matcher(query).replaceAll("");
    query = WHERE_CONDITIONS.matcher(query).replaceAll("");
    return query;
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
    public static final String PATTERN_TO_REPLACE = "patternToReplace";
    public static final String REPLACE_WITH = "replaceWith";
    public static final String FETCH_SIZE = "fetchSize";

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

    @Nullable
    @Name(PATTERN_TO_REPLACE)
    @Description("The pattern to replace in the field name in the table, it is typically used with the " +
                   "Replace With config. If Replace With is not set, the pattern will be removed in the field name.")
    String patternToReplace;

    @Nullable
    @Name(REPLACE_WITH)
    @Description("The string that will be replaced in the field name in the table, it must be used with the " +
                   "Pattern To Replace config.")
    String replaceWith;

    @Nullable
    @Name(FETCH_SIZE)
    @Macro
    @Description("The number of rows to fetch at a time per split. Larger fetch size can result in faster import, " +
                  "with the tradeoff of higher memory usage.")
    Integer fetchSize;

    @Nullable
    private String getImportQuery() {
      return cleanQuery(importQuery);
    }

    private String getBoundingQuery() {
      return cleanQuery(boundingQuery);
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    private void validate(FailureCollector collector) {
      boolean hasOneSplit = false;
      if (!containsMacro(NUM_SPLITS) && numSplits != null) {
        if (numSplits < 1) {
          collector.addFailure("Number of Splits must be a positive number.", null).withConfigProperty(NUM_SPLITS);
        }
        if (numSplits == 1) {
          hasOneSplit = true;
        }
      }

      if (!containsMacro(TRANSACTION_ISOLATION_LEVEL) && transactionIsolationLevel != null) {
        TransactionIsolationLevel.validate(transactionIsolationLevel, collector);
      }

      if (!containsMacro(IMPORT_QUERY) && Strings.isNullOrEmpty(importQuery)) {
        collector.addFailure("Import Query must be specified.", null).withConfigProperty(IMPORT_QUERY);
      }

      if (!hasOneSplit && !containsMacro(IMPORT_QUERY) && !Strings.isNullOrEmpty(importQuery) &&
        !getImportQuery().contains("$CONDITIONS")) {
        collector.addFailure("Invalid Import Query.", String.format("Import Query %s must contain the " +
                                                                      "string '$CONDITIONS'.", importQuery))
          .withConfigProperty(IMPORT_QUERY);
      }

      if (!hasOneSplit && !containsMacro(SPLIT_BY) && Strings.isNullOrEmpty(splitBy)) {
        collector.addFailure("Split-By Field Name must be specified if Number of Splits is not set to 1.",
                             null).withConfigProperty(SPLIT_BY).withConfigProperty(NUM_SPLITS);
      }

      if (!hasOneSplit && !containsMacro(BOUNDING_QUERY) && Strings.isNullOrEmpty(boundingQuery)) {
        collector.addFailure("Bounding Query must be specified if Number of Splits is not set to 1.", null)
          .withConfigProperty(BOUNDING_QUERY).withConfigProperty(NUM_SPLITS);
      }

      if (replaceWith != null && patternToReplace == null) {
        collector.addFailure("Replace With is set but Pattern To Replace is not provided", null)
          .withConfigProperty(REPLACE_WITH).withConfigProperty(PATTERN_TO_REPLACE);
      }

      if (!containsMacro(FETCH_SIZE) && fetchSize != null && fetchSize <= 0) {
        collector.addFailure("Invalid fetch size.", "Fetch size must be a positive integer.")
          .withConfigProperty(FETCH_SIZE);
      }
    }

    @Nullable
    private Schema getSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure(String.format("Invalid Schema : %s", e.getMessage()), null);
      }
      throw collector.getOrThrowException();
    }
  }

  private void emitLineage(BatchSourceContext context) {
    Schema schema = sourceConfig.getSchema(context.getFailureCollector());
    if (schema == null) {
      schema = context.getOutputSchema();
    }
    LineageRecorder lineageRecorder = new LineageRecorder(context, sourceConfig.referenceName);
    lineageRecorder.createExternalDataset(schema);

    if (schema != null && schema.getFields() != null) {
      lineageRecorder.recordRead("Read", "Read from DB.",
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
