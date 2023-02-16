/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.common.db;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.db.schemareader.CommonSchemaReader;
import io.cdap.plugin.common.util.ExceptionUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Common logic for database related connector.
 *
 * @param <T> type of the plugin config
 */
public abstract class AbstractDBConnector<T extends PluginConfig & DBConnectorProperties> implements Connector {
  public static final String ENTITY_TYPE_DATABASE = "database";
  public static final String ENTITY_TYPE_SCHEMA = "schema";

  private static final String RESULTSET_COLUMN_TABLE_NAME = "TABLE_NAME";
  private static final String RESULTSET_COLUMN_TABLE_TYPE = "TABLE_TYPE";
  private static final String RESULTSET_COLUMN_TABLE_SCHEM = "TABLE_SCHEM";
  private static final String RESULTSET_COLUMN_DATA_TYPE = "DATA_TYPE";
  private static final String RESULTSET_COLUMN_TYPE_NAME = "TYPE_NAME";
  private static final String RESULTSET_COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
  private static final String RESULTSET_COLUMN_COLUMN_SIZE = "COLUMN_SIZE";
  private static final String RESULTSET_COLUMN_COLUMN_NAME = "COLUMN_NAME";
  private static final String RESULTSET_COLUMN_IS_NULLABLE = "IS_NULLABLE";
  protected static final String RESULTSET_COLUMN_TABLE_CAT = "TABLE_CAT";


  private final T config;
  private DriverCleanup driverCleanup;
  protected Class<? extends Driver> driverClass;

  protected AbstractDBConnector(T config) {
    this.config = config;
  }

  @Override
  public void configure(ConnectorConfigurer configurer) throws IOException {
    driverClass = DBUtils.loadJDBCDriverClass(
      configurer, config.getJdbcPluginName(), DBUtils.PLUGIN_TYPE_JDBC,
      String.format("connector.jdbc.%s", config.getJdbcPluginName()), null);

    try {
      driverCleanup =
        DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(), config.getJdbcPluginName(),
                                            DBUtils.PLUGIN_TYPE_JDBC);
    } catch (Exception e) {
      throw new IOException(
        String.format("Failed to register JDBC driver Class, connection string : %s, JDBC plugin name : %s.",
                      config.getConnectionString(), config.getJdbcPluginName()), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (driverCleanup != null) {
      driverCleanup.destroy();
    }
  }

  @Override
  public void test(ConnectorContext context) throws ValidationException {
    try (Connection connection = getConnection()) {
      connection.getMetaData();
    } catch (Exception e) {
      context.getFailureCollector().addFailure(e.getMessage(),
        "Make sure you specify the correct connection properties.").withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
    DBConnectorPath dbConnectorPath = getDBConnectorPath(request.getPath());
    try (Connection connection = getConnection(dbConnectorPath)) {
      int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
      if (dbConnectorPath.isRoot() && dbConnectorPath.containDatabase()) {
        return listDatabases(connection, limit);
      }
      if (dbConnectorPath.containSchema() && dbConnectorPath.getSchema() == null) {
        return listSchemas(connection, dbConnectorPath.getDatabase(), limit);
      }
      if (dbConnectorPath.getTable() == null) {
        return listTables(connection, dbConnectorPath.getDatabase(), dbConnectorPath.getSchema(), limit);
      }
      return getTableDetail(connection, dbConnectorPath.getDatabase(), dbConnectorPath.getSchema(),
                            dbConnectorPath.getTable());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to browse for path %s. Error: %s", request.getPath(),
                                          ExceptionUtils.getRootCauseMessage(e)), e);
    }
  }

  protected abstract DBConnectorPath getDBConnectorPath(String path) throws IOException;

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext,
                                    ConnectorSpecRequest request) throws IOException {
    DBConnectorPath dbConnectorPath = getDBConnectorPath(request.getPath());
    try (Connection connection = getConnection(dbConnectorPath)) {
      ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
      setConnectorSpec(request, dbConnectorPath, specBuilder);
      String table = dbConnectorPath.getTable();
      if (table == null) {
        return specBuilder.build();
      }
      String database = dbConnectorPath.getDatabase();
      if (database == null) {
        database = connection.getCatalog();
      } else {
        validateDatabase(database, connection);
      }
      String schema = dbConnectorPath.getSchema();
      validateSchema(database, schema, connection);
      Schema outputSchema = getTableSchema(connection, database, schema, table);
      return specBuilder.setSchema(outputSchema).build();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to generate spec for path %s. Error: %s.", request.getPath(),
                                          ExceptionUtils.getRootCauseMessage(e)), e);
    }
  }

  protected Schema getTableSchema(Connection connection, String database,
                                  String schema, String table) throws SQLException {
    ResultSet columns = connection.getMetaData().getColumns(database, schema, table, null);
    List<Schema.Field> fields = new ArrayList<>();
    while (columns.next()) {
      int sqlType = columns.getInt(RESULTSET_COLUMN_DATA_TYPE);
      String typeName = columns.getString(RESULTSET_COLUMN_TYPE_NAME);
      int scale = columns.getInt(RESULTSET_COLUMN_DECIMAL_DIGITS);
      int precision = columns.getInt(RESULTSET_COLUMN_COLUMN_SIZE);
      String columnName = columns.getString(RESULTSET_COLUMN_COLUMN_NAME);
      boolean isSigned = typeName.toLowerCase().indexOf("unsigned") < 0;
      Schema columnSchema =
              new CommonSchemaReader().
                      getSchema(columnName, sqlType, typeName, "", precision, scale, isSigned);
      String isNullable = columns.getString(RESULTSET_COLUMN_IS_NULLABLE);
      if ("YES".equals(isNullable)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      fields.add(Schema.Field.of(columnName, columnSchema));
    }
    Schema outputSchema = Schema.recordOf("output", fields);
    return outputSchema;
  }

  /**
   * Override this method to provide related plugins, properties or schema
   *
   * @param request the spec generation request
   * @param path    the db connector path
   * @param builder the builder of the spec
   */
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path, ConnectorSpec.Builder builder) {
    // no-op
  }

  protected BrowseDetail listDatabases(Connection connection, int limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    ResultSet databaseResultSet;
    databaseResultSet = queryDatabases(connection);
    while (databaseResultSet.next()) {
      if (count < limit) {
        String name = databaseResultSet.getString(RESULTSET_COLUMN_TABLE_CAT);
        browseDetailBuilder
          .addEntity(BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_DATABASE).canBrowse(true).build());
      }
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  protected ResultSet queryDatabases(Connection connection) throws SQLException {
    return connection.getMetaData().getCatalogs();
  }

  protected BrowseDetail listSchemas(Connection connection, @Nullable String database, int limit)
    throws SQLException {
    boolean containsDatabase = false;
    if (database == null) {
      database = connection.getCatalog();
    } else {
      validateDatabase(database, connection);
      containsDatabase = true;
    }
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    try (ResultSet resultSet = connection.getMetaData().getSchemas(database, null)) {
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_SCHEM);
        if (count >= limit) {
          break;
        }
        browseDetailBuilder.addEntity(
          BrowseEntity.builder(name, containsDatabase ? "/" + database + "/" + name : "/" + name, ENTITY_TYPE_SCHEMA)
            .canBrowse(true).build());
        count++;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  protected BrowseDetail getTableDetail(Connection connection, @Nullable String database, @Nullable String schema,
                                        String table) throws SQLException {
    boolean containsDatabase = false;
    if (database == null) {
      database = connection.getCatalog();
    } else {
      // make sure database exists
      validateDatabase(database, connection);
      containsDatabase = true;
    }

    // make sure schema exists
    validateSchema(database, schema, connection);
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    try (ResultSet resultSet = connection.getMetaData().getTables(database, schema, table, null)) {
      if (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
        browseDetailBuilder.addEntity(
          BrowseEntity.builder(
            name,
            (containsDatabase ? "/" + database : "") + (schema == null ? "" : "/" + schema) + "/" + name,
            resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE)
          ).canSample(true).build());
      } else {
        throw new IllegalArgumentException(String.format("Cannot find table : %s.%s.", schema, table));
      }
    }
    return browseDetailBuilder.setTotalCount(1).build();
  }

  protected BrowseDetail listTables(Connection connection, @Nullable String database, @Nullable String schema,
                                    int limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    boolean containsDatabase = false;
    if (database == null) {
      database = connection.getCatalog();
    } else {
      // make sure database exists
      validateDatabase(database, connection);
      containsDatabase = true;
    }
    // make sure schema exists
    validateSchema(database, schema, connection);

    int count = 0;
    try (ResultSet resultSet = connection.getMetaData().getTables(database, schema, null, null)) {
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
        if (count >= limit) {
          break;
        }
        browseDetailBuilder.addEntity(
          BrowseEntity.builder(
            name,
            (containsDatabase ? "/" + database : "") + (schema == null ? "" : "/" + schema) + "/" + name,
            resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE).toLowerCase()
          ).canSample(true).build());
        count++;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private void validateDatabase(String database, Connection connection) throws SQLException {
    try (ResultSet catalogs = queryDatabases(connection)) {
      boolean exits = false;
      while (catalogs.next()) {
        if (database.equals(catalogs.getString(RESULTSET_COLUMN_TABLE_CAT))) {
          exits = true;
          break;
        }
      }
      if (!exits) {
        throw new IllegalArgumentException(String.format("Database %s does not exist.", database));
      }
    }
  }

  protected void validateSchema(String database, String schema, Connection connection)
    throws SQLException {
    if (schema == null) {
      return;
    }
    // NOTE Oracle schema name is case sensitive here
    try (ResultSet schemas = connection.getMetaData().getSchemas(database, schema)) {
      if (!schemas.next()) {
        throw new IllegalArgumentException(String.format("Schema '%s' does not exist.", schema));
      }
    }
  }

  protected Connection getConnection() {
    return getConnection(config.getConnectionString(), config.getConnectionArgumentsProperties());
  }

  protected Connection getConnection(DBConnectorPath path) {
    return getConnection();
  }

  protected Connection getConnection(String connectionString, Properties connectionArguments) {
    try {
      return DriverManager.getConnection(connectionString, connectionArguments);
    } catch (SQLException e) {
      Properties args = (Properties) connectionArguments.clone();
      args.remove("password");
      throw new IllegalArgumentException(String.format("Failed to create connection to database via connection string" +
                                                         ": %s and arguments: %s. Error: %s.", connectionString,
                                                       args, ExceptionUtils.getRootCauseMessage(e), e));
    }
  }
}
