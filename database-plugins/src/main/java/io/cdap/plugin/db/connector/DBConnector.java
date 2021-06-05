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
 */

package io.cdap.plugin.db.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.DBUtils;
import io.cdap.plugin.DriverCleanup;
import io.cdap.plugin.db.batch.source.DBSource;
import io.cdap.plugin.db.common.DBBaseConfig;
import io.cdap.plugin.db.common.DBDifferenceUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * A Generic Database Connector that connects to database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(DBConnector.NAME)
@Description("This connector creates connections to a Database.")
public class DBConnector implements DirectConnector {
  public static final String NAME = "Database";
  public static final String ENTITY_TYPE_DATABASE = "DATABASE";
  public static final String ENTITY_TYPE_SCHEMA = "SCHEMA";
  public static final String[] SUPPORTED_TYPES = new String[]{"TABLE", "VIEW", "MATERIALIZED VIEW"};

  private static final String RESULTSET_COLUMN_TABLE_NAME = "TABLE_NAME";
  private static final String RESULTSET_COLUMN_TABLE_TYPE = "TABLE_TYPE";
  private static final String RESULTSET_COLUMN_TABLE_SCHEM = "TABLE_SCHEM";
  private static final String RESULTSET_COLUMN_DATA_TYPE = "DATA_TYPE";
  private static final String RESULTSET_COLUMN_TYPE_NAME = "TYPE_NAME";
  private static final String RESULTSET_COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
  private static final String RESULTSET_COLUMN_COLUMN_SIZE = "COLUMN_SIZE";
  private static final String RESULTSET_COLUMN_COLUMN_NAME = "COLUMN_NAME";
  private static final String RESULTSET_COLUMN_IS_NULLABLE = "IS_NULLABLE";

  private DBConnectorConfig config;
  private DriverCleanup driverCleanup;

  DBConnector(DBConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void configure(PluginConfigurer configurer) {
    Class<? extends Driver> driverClass = DBUtils.loadJDBCDriverClass(configurer, config.getJdbcPluginName(),
      String.format("connector.jdbc.%s", config.getJdbcPluginName()), null);
    try {
      driverCleanup =
        DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(), config.getJdbcPluginName());
    } catch (Exception e) {
      throw new RuntimeException(String
        .format("Failed to register JDBC driver Class, connection string : %s, JDBC plugin name : %s.",
          config.getConnectionString(), config.getJdbcPluginName()), e);
    }
  }

  @Override
  public void test(FailureCollector collector) throws ValidationException {
    try {
      java.sql.Connection connection = getConnection(config.getConnectionString(), config.getAllConnectionArguments());
      connection.getMetaData();
    } catch (Exception e) {
      collector.addFailure(String.format("Failed to connect to the database : %s.", e.getMessage()),
        "Make sure you " + "specify the correct connection properties.").withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public BrowseDetail browse(BrowseRequest browseRequest) throws IOException {
    return connectAndQuery(browseRequest.getPath(), "browse", (connection, path) -> {
      String database = path.getDatabase();
      if (database == null) {
        // browse project to list all databases
        return listDatabases(connection, browseRequest.getLimit());
      }
      String schema = path.getSchema();
      if (schema == null && path.supportSchema()) {
        return listSchemas(connection, database, browseRequest.getLimit());
      }
      String table = path.getTable();
      if (table == null) {
        return listTables(connection, database, schema, browseRequest.getLimit());
      }
      return getTableDetail(connection, database, schema, table);
    });
  }

  @Override
  public List<StructuredRecord> sample(SampleRequest sampleRequest) throws IOException {
    return connectAndQuery(sampleRequest.getPath(), "sample", (connection, path) -> {
      String table = path.getTable();
      if (table == null) {
        throw new IllegalArgumentException("Path should contain table name.");
      }
      String schema = path.getSchema();
      return getTableData(connection, schema, table, sampleRequest.getLimit());
    });
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorSpecRequest connectorSpecRequest) throws IOException {

    return connectAndQuery(connectorSpecRequest.getPath(), "generate spec", (connection, path) -> {
      ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
      DatabaseMetaData metaData = connection.getMetaData();
      Map<String, String> properties = new HashMap<>();
      //we allow users to browse different database than the one specified in the connection string
      String connectionStr = metaData.getURL();
      if (!config.getConnectionString().equals(connectionStr)) {
        properties.put(DBBaseConfig.NAME_USE_CONNECTION, "false");
        properties.put(DBConnectorConfig.CONNECTION_STRING, connectionStr);
        properties.put(DBConnectorConfig.CONNECTION_ARGUMENTS, config.getConnectionArguments());
        properties.put(DBConnectorConfig.JDBC_PLUGIN_NAME, config.getJdbcPluginName());
        properties.put(DBConnectorConfig.USER, config.getUser());
        properties.put(DBConnectorConfig.PASSWORD, config.getPassword());
      } else {
        properties.put(DBBaseConfig.NAME_USE_CONNECTION, "true");
        properties.put(DBBaseConfig.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
      }
      String table = path.getTable();
      if (table != null) {
        String database = path.getDatabase();
        String schema = path.getSchema();
        ResultSet columns = metaData.getColumns(database, schema, table, null);
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
        specBuilder.setSchema(Schema.recordOf("output", fields));
        properties.put(DBSource.DBSourceConfig.IMPORT_QUERY,
          schema == null ? String.format("SELECT * FROM %s;", table) :
            String.format("SELECT * FROM %s.%s;", schema, table));
        properties.put(DBSource.DBSourceConfig.NUM_SPLITS, "1");
      }
      return specBuilder.addRelatedPlugin(new PluginSpec(DBSource.NAME, BatchSource.PLUGIN_TYPE, properties)).build();
    });
  }

  @Override
  public void close() throws IOException {
    if (driverCleanup != null) {
      driverCleanup.destroy();
    }
  }

  private Connection getConnection(String connectionString, Properties arguments) {
    try {
      return DriverManager.getConnection(connectionString, arguments);
    } catch (SQLException e) {
      throw new IllegalArgumentException(String.format("Cannot connect to database via connection string : %s and " +
        "arguments: %s. Make sure you have correct connection properties.", connectionString, arguments.toString()), e);
    }
  }

  private BrowseDetail listDatabases(Connection connection, @Nullable Integer limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    ResultSet databaseResultSet;
    databaseResultSet = DBDifferenceUtils.getDatabases(connection);
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    while (databaseResultSet.next()) {
      if (count < countLimit) {
        String name = databaseResultSet.getString(DBDifferenceUtils.RESULTSET_COLUMN_TABLE_CAT);
        browseDetailBuilder
          .addEntity(BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_DATABASE).canBrowse(true).build());
      }
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail listSchemas(Connection connection, String database, @Nullable Integer limit)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getSchemas();
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    while (resultSet.next()) {
      String name = resultSet.getString(RESULTSET_COLUMN_TABLE_SCHEM);
      if (count < countLimit) {
        browseDetailBuilder.addEntity(
          BrowseEntity.builder(name, "/" + database + "/" + name, ENTITY_TYPE_SCHEMA).canBrowse(true).build());
      }
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail getTableDetail(Connection connection, String database, @Nullable String schema, String table)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(database, schema, table, SUPPORTED_TYPES);
    if (resultSet.next()) {
      String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
      browseDetailBuilder.addEntity(BrowseEntity
        .builder(name, schema == null ? "/" + database + "/" + name : "/" + database + "/" + schema + "/" + name,
          resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE)).canSample(true).build());
    } else {
      throw new IllegalArgumentException(String.format("Cannot find table : %s.%s.%s.", database, schema, table));
    }
    return browseDetailBuilder.setTotalCount(1).build();
  }

  private BrowseDetail listTables(Connection connection, String database, @Nullable String schema,
    @Nullable Integer limit) throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    DatabaseMetaData metaData = connection.getMetaData();
    // make sure schema exists
    if (schema != null) {
      // NOTE Oracle schema name is case sensitive here
      ResultSet schemas = metaData.getSchemas(database, schema);
      if (!schemas.next()) {
        throw new IllegalArgumentException(String.format("Schema '%s' does not exist.", schema));
      }
    }
    ResultSet resultSet = metaData.getTables(database, schema, null, SUPPORTED_TYPES);
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    while (resultSet.next()) {
      String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
      if (count < countLimit) {
        browseDetailBuilder.addEntity(BrowseEntity
          .builder(name, schema == null ? "/" + database + "/" + name : "/" + database + "/" + schema + "/" + name,
            resultSet.getString(RESULTSET_COLUMN_TABLE_TYPE)).canSample(true).build());
      }
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private List<StructuredRecord> getTableData(Connection connection, @Nullable String schema,
    String table, int limit) throws SQLException {
    String productName = connection.getMetaData().getDatabaseProductName();
    String query = DBDifferenceUtils.getTableQueryWithLimit(productName, schema, table, limit);
    return DBUtils.parseResultSet(connection.createStatement().executeQuery(query));
  }

  private <T> T connectAndQuery(String pathStr, String operation, Query<T> query) throws IOException {
    // parse path as support schema first to get database info
    DBPath path = new DBPath(pathStr, true);
    // Some Database don't support multiple catalog in one connection
    // that means if database is encoded in the connection string, you can only query schemas or tables under that
    // database (e.g. PostgreSQL) , so need to replace the database info in the connection string
    String database = path.getDatabase();
    String connectionString = config.getConnectionString();
    connectionString = DBDifferenceUtils.replaceDatabase(connectionString, database);
    try (Connection connection = getConnection(connectionString, config.getAllConnectionArguments())) {
      if (!connection.getMetaData().supportsSchemasInTableDefinitions()) {
        path = new DBPath(pathStr, false);
      }
      return query.run(connection, path);

    } catch (SQLException e) {
      throw new IOException(String.format("Failed to %s.", operation), e);
    }
  }
  @FunctionalInterface
  interface Query<T> {
    T run(Connection connection, DBPath path) throws SQLException;
  }
}
