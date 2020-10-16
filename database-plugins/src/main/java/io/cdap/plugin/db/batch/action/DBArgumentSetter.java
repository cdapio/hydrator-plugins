/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.db.batch.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.plugin.BaseConnectionConfig;
import io.cdap.plugin.DBManager;
import io.cdap.plugin.DBUtils;
import io.cdap.plugin.DriverCleanup;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Action that converts db column into pipeline argument.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("DatabaseArgumentSetter")
@Description("Reads single record from database table and converts it to arguments for pipeline")
public class DBArgumentSetter extends Action {
  private static final String JDBC_PLUGIN_ID = "driver";
  private final DBArgumentSetterConfig config;

  public DBArgumentSetter(DBArgumentSetterConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    FailureCollector failureCollector = context.getFailureCollector();
    SettableArguments settableArguments = context.getArguments();
    processArguments(driverClass, failureCollector, settableArguments);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer)
    throws IllegalArgumentException {
    DBManager dbManager = new DBManager(config);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Class<? extends Driver> driverClass = dbManager.validateJDBCPluginPipeline(pipelineConfigurer,
                                                                               JDBC_PLUGIN_ID, collector);
    config.validate(collector);
    if (config.fieldsContainMacro()) {
      return;
    }
    try {
      processArguments(driverClass, collector, null);
    } catch (SQLException e) {
      collector.addFailure("SQL error while executing query: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
    } catch (IllegalAccessException | InstantiationException e) {
      collector.addFailure("Unable to instantiate JDBC driver: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Creates connection to database. Reads row from database based on selection conditions and
   * depending on whether settable arguments is provided or not set the argument from row columns.
   *
   * @param driverClass       {@link Class<? extends Driver>}
   * @param failureCollector  {@link FailureCollector}
   * @param settableArguments {@link SettableArguments}
   * @throws SQLException           is raised when there is sql related exception
   * @throws IllegalAccessException is raised when there is access related exception
   * @throws InstantiationException is raised when there is class/driver issue
   */
  private void processArguments(Class<? extends Driver> driverClass,
                                FailureCollector failureCollector, SettableArguments settableArguments)
    throws SQLException, IllegalAccessException, InstantiationException {
    DriverCleanup driverCleanup;
    driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.connectionString, config.jdbcPluginType,
                                                        config.jdbcPluginName);
    Properties connectionProperties = new Properties();
    connectionProperties.putAll(config.getConnectionArguments());
    try {
      Connection connection = DriverManager
        .getConnection(config.connectionString, connectionProperties);
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(config.getQuery());
      boolean hasRecord = resultSet.next();
      if (!hasRecord) {
        failureCollector.addFailure("No record found.",
                                    "The argument selection conditions must match only one record.");
        return;
      }
      if (settableArguments != null) {
        setArguments(resultSet, failureCollector, settableArguments);
      }
      if (resultSet.next()) {
        failureCollector
          .addFailure("More than one records found.",
                      "The argument selection conditions must match only one record.");
      }
    } finally {
      driverCleanup.destroy();
    }
  }

  /**
   * Converts column from jdbc results set into pipeline arguments
   *
   * @param resultSet        - result set from db {@link ResultSet}
   * @param failureCollector - context failure collector @{link FailureCollector}
   * @param arguments        - context argument setter {@link SettableArguments}
   * @throws SQLException - raises {@link SQLException} when configuration is not valid
   */
  private void setArguments(ResultSet resultSet, FailureCollector failureCollector,
                            SettableArguments arguments) throws SQLException {
    String[] columns = config.getArgumentsColumns().split(",");
    for (String column : columns) {
      arguments.set(column, resultSet.getString(column));
    }
  }

  /**
   * Config for ArgumentSetter reading from database
   */
  public static class DBArgumentSetterConfig extends BaseConnectionConfig {

    public static final String DATABASE_NAME = "databaseName";
    public static final String TABLE_NAME = "tableName";
    public static final String ARGUMENT_SELECTION_CONDITIONS = "argumentSelectionConditions";
    public static final String ARGUMENTS_COLUMNS = "argumentsColumns";

    @Name(DATABASE_NAME)
    @Description("The name of the database which contains\n"
      + "the configuration table")
    @Macro
    String databaseName;

    @Name(TABLE_NAME)
    @Description("The name of the table in the database\n"
      + "containing the configurations for the pipeline")
    @Macro
    String tableName;

    @Name(ARGUMENT_SELECTION_CONDITIONS)
    @Description("A set of conditions for identifying the\n"
      + "arguments to run a pipeline. Users can\n"
      + "specify multiple conditions in the format\n"
      + "column1=<column1-value>;column2=<colum\n"
      + "n2-value>. A particular use case for this\n"
      + "would be feed=marketing AND\n"
      + "date=20200427. The conditions specified\n"
      + "should be logically ANDed to determine the\n"
      + "arguments for a run. When the conditions are\n"
      + "applied, the table should return exactly 1 row.\n"
      + "If it doesn’t return any rows, or if it returns\n"
      + "multiple rows, the pipeline should abort with\n"
      + "appropriate errors. Typically, users should\n"
      + "use macros in this field, so that they can\n"
      + "specify the conditions at runtime.")
    @Macro
    String argumentSelectionConditions;

    @Name(ARGUMENTS_COLUMNS)
    @Description("Names of the columns that contain the\n"
      + "arguments for this run. The values of this\n"
      + "columns in the row that satisfies the argument\n"
      + "selection conditions determines the\n"
      + "arguments for the pipeline run")
    @Macro
    String argumentsColumns;

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }

    public String getArgumentSelectionConditions() {
      return argumentSelectionConditions;
    }

    public String getArgumentsColumns() {
      return argumentsColumns;
    }

    public String getQuery() {
      if (this.getArgumentSelectionConditions() == null) {
        throw new IllegalArgumentException("Argument selection conditions are empty.");
      }
      String[] split = this.getArgumentSelectionConditions().split(";");
      String conditions = String.join(" AND ", split);

      return String
        .format("SELECT %s FROM %s WHERE %s", this.getArgumentsColumns(), this.getTableName(),
                conditions);
    }

    /**
     * Validates config input fields.
     *
     * @param collector context failure collector {@link FailureCollector}
     */
    public void validate(FailureCollector collector) {
      if (!containsMacro(CONNECTION_STRING) && Strings.isNullOrEmpty(this.connectionString)) {
        collector.addFailure("Invalid connection string.", "Connection string cannot be empty.");
      }
      if (!containsMacro(BaseConnectionConfig.USER) && Strings.isNullOrEmpty(this.user)) {
        collector.addFailure("Invalid username.", "Username cannot be empty.");
      }
      if (!containsMacro(BaseConnectionConfig.PASSWORD) && Strings.isNullOrEmpty(this.password)) {
        collector.addFailure("Invalid password.", "Password cannot be empty.");
      }
      if (!containsMacro(DATABASE_NAME) && Strings.isNullOrEmpty(this.getDatabaseName())) {
        collector.addFailure("Invalid database.", "Valid database must be specified.");
      }
      if (!containsMacro(TABLE_NAME) && Strings.isNullOrEmpty(this.getTableName())) {
        collector.addFailure("Invalid table.", "Valid table must be specified.");
      }
      if (!containsMacro(ARGUMENTS_COLUMNS) && Strings.isNullOrEmpty(this.getArgumentsColumns())) {
        collector
          .addFailure("Invalid arguments columns.", "Arguments column names must be specified.");
      }
      if (!containsMacro(ARGUMENT_SELECTION_CONDITIONS) && Strings
        .isNullOrEmpty(this.getArgumentSelectionConditions())) {
        collector.addFailure("Invalid conditions.", "Filter conditions must be specified.");
      }
      collector.getOrThrowException();
    }

    public boolean fieldsContainMacro() {
      return containsMacro(CONNECTION_STRING)
        || containsMacro(BaseConnectionConfig.USER)
        || containsMacro(BaseConnectionConfig.PASSWORD)
        || containsMacro(DATABASE_NAME)
        || containsMacro(TABLE_NAME)
        || containsMacro(ARGUMENTS_COLUMNS)
        || containsMacro(ARGUMENT_SELECTION_CONDITIONS);
    }
  }
}
