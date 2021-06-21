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
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Common logic for database related connector.
 *
 * @param <T> type of the plugin config
 */
public abstract class AbstractDBConnector<T extends PluginConfig & DBConnectorProperties> implements Connector {
  public static final String ENTITY_TYPE_DATABASE = "DATABASE";
  public static final String ENTITY_TYPE_SCHEMA = "SCHEMA";

  private static final String RESULTSET_COLUMN_TABLE_NAME = "TABLE_NAME";
  private static final String RESULTSET_COLUMN_TABLE_TYPE = "TABLE_TYPE";
  private static final String RESULTSET_COLUMN_TABLE_SCHEM = "TABLE_SCHEM";
  private static final String RESULTSET_COLUMN_DATA_TYPE = "DATA_TYPE";
  private static final String RESULTSET_COLUMN_TYPE_NAME = "TYPE_NAME";
  private static final String RESULTSET_COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
  private static final String RESULTSET_COLUMN_COLUMN_SIZE = "COLUMN_SIZE";
  private static final String RESULTSET_COLUMN_COLUMN_NAME = "COLUMN_NAME";
  private static final String RESULTSET_COLUMN_IS_NULLABLE = "IS_NULLABLE";
  private static final String RESULTSET_COLUMN_TABLE_CAT = "TABLE_CAT";


  private final T config;
  private DriverCleanup driverCleanup;

  protected AbstractDBConnector(T config) {
    this.config = config;
  }

  @Override
  public void configure(ConnectorConfigurer configurer) throws IOException {
    Class<? extends Driver> driverClass = DBUtils.loadJDBCDriverClass(
      configurer, config.getJdbcPluginName(), String.format("connector.jdbc.%s", config.getJdbcPluginName()), null);

    try {
      driverCleanup =
        DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(), config.getJdbcPluginName());
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
      context.getFailureCollector().addFailure(
        String.format("Failed to connect to the database : %s.", e.getMessage()),
        "Make sure you " + "specify the correct connection properties.").withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
    try (Connection connection = getConnection()) {
      DBConnectorPath dbConnectorPath = getDBConnectorPath(connection, request.getPath());
      int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
      if (dbConnectorPath.isRoot() && dbConnectorPath.containDatabase()) {
        return listDatabases(connection, limit);
      }
      if (dbConnectorPath.containSchema() && dbConnectorPath.getSchema() == null) {
        return listSchemas(connection, limit);
      }
      if (dbConnectorPath.getTable() == null) {
        return listTables(connection, dbConnectorPath.getSchema(), limit);
      }
      return getTableDetail(connection, dbConnectorPath.getSchema(), dbConnectorPath.getTable());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to browse for path %s", request.getPath()), e);
    }
  }

  protected abstract DBConnectorPath getDBConnectorPath(Connection connection, String path) throws IOException;

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext,
                                    ConnectorSpecRequest request) throws IOException {

    try (Connection connection = getConnection()) {
      DBConnectorPath dbConnectorPath = getDBConnectorPath(connection, request.getPath());
      ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
      DatabaseMetaData metaData = connection.getMetaData();
      String table = dbConnectorPath.getTable();
      setConnectorSpec(request, dbConnectorPath, specBuilder);
      if (table == null) {
        return specBuilder.build();
      }

      String schema = dbConnectorPath.getSchema();
      ResultSet columns = metaData.getColumns(connection.getCatalog(), schema, table, null);
      List<Schema.Field> fields = new ArrayList<>();
      while (columns.next()) {
        int sqlType = columns.getInt(RESULTSET_COLUMN_DATA_TYPE);
        String typeName = columns.getString(RESULTSET_COLUMN_TYPE_NAME);
        int scale = columns.getInt(RESULTSET_COLUMN_DECIMAL_DIGITS);
        int precision = columns.getInt(RESULTSET_COLUMN_COLUMN_SIZE);
        String columnName = columns.getString(RESULTSET_COLUMN_COLUMN_NAME);
        Schema columnSchema = DBUtils.getSchema(typeName, sqlType, precision, scale, columnName, true);
        String isNullable = columns.getString(RESULTSET_COLUMN_IS_NULLABLE);
        if ("YES".equals(isNullable)) {
          columnSchema = Schema.nullableOf(columnSchema);
        }
        fields.add(Schema.Field.of(columnName, columnSchema));
      }
      return specBuilder.setSchema(Schema.recordOf("output", fields)).build();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to generate spec for path %s", request.getPath()), e);
    }
  }

  /**
   * Override this method to provide related plugins, properties or schema
   *
   * @param request the spec generation request
   * @param path the db connector path
   * @param builder the builder of the spec
   */
  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path, ConnectorSpec.Builder builder) {
    // no-op
  }

  protected BrowseDetail listDatabases(Connection connection, int limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    ResultSet databaseResultSet;
    databaseResultSet = connection.getMetaData().getCatalogs();
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

  protected BrowseDetail listSchemas(Connection connection, int limit)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getSchemas()) {
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_SCHEM);
        if (count >= limit) {
          break;
        }

        browseDetailBuilder.addEntity(
          BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_SCHEMA).canBrowse(true).build());
        count++;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  protected BrowseDetail getTableDetail(Connection connection, @Nullable String schema, String table)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), schema, table, null)) {
      if (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
        browseDetailBuilder.addEntity(BrowseEntity
                                        .builder(name, schema == null ? "/" + name : "/" + schema + "/" + name,
                                                 resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE)).canSample(true)
                                        .build());
      } else {
        throw new IllegalArgumentException(String.format("Cannot find table : %s.%s.", schema, table));
      }
    }
    return browseDetailBuilder.setTotalCount(1).build();
  }

  protected BrowseDetail listTables(Connection connection, @Nullable String schema, int limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    DatabaseMetaData metaData = connection.getMetaData();
    // make sure schema exists
    if (schema != null) {
      // NOTE Oracle schema name is case sensitive here
      try (ResultSet schemas = metaData.getSchemas(connection.getCatalog(), schema)) {
        if (!schemas.next()) {
          throw new IllegalArgumentException(String.format("Schema '%s' does not exist.", schema));
        }
      }
    }
    try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), schema, null, null)) {
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
        if (count >= limit) {
          break;
        }
        browseDetailBuilder.addEntity(BrowseEntity
                                        .builder(name, schema == null ? "/" + name : "/" + schema + "/" + name,
                                                 resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE)).canSample(true)
                                        .build());
        count++;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  protected Connection getConnection() {
    try {
      return DriverManager.getConnection(config.getConnectionString(), config.getConnectionArgumentsProperties());
    } catch (SQLException e) {
      throw new IllegalArgumentException(String.format("Cannot connect to database via connection string : %s and " +
                                                         "arguments: %s. Make sure you have correct connection " +
                                                         "properties.",
                                                       config.getConnectionString(),
                                                       config.getConnectionArgumentsProperties()), e);
    }
  }
}
