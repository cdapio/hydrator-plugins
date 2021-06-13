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

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A Generic Database Connector that connects to database via JDBC.
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(DBConnector.NAME)
@Description("This connector creates connections to a Database.")
@Category("Database")
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
  public void configure(ConnectorConfigurer configurer) {
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
  public void test(ConnectorContext context) throws ValidationException {
    try {
      java.sql.Connection connection = getConnection();
      connection.getMetaData();
    } catch (Exception e) {
      context.getFailureCollector().addFailure(String.format("Failed to connect to the database : %s.", e.getMessage()),
        "Make sure you " + "specify the correct connection properties.").withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext context, BrowseRequest browseRequest) throws IOException {
    return connectAndQuery(browseRequest.getPath(), "browse", (connection, path) -> {
      String schema = path.getSchema();
      if (schema == null && path.supportSchema()) {
        return listSchemas(connection, browseRequest.getLimit());
      }
      String table = path.getTable();
      if (table == null) {
        return listTables(connection, schema, browseRequest.getLimit());
      }
      return getTableDetail(connection, schema, table);
    });
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext context, SampleRequest sampleRequest) throws IOException {
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
  public ConnectorSpec generateSpec(ConnectorContext context, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {

    return connectAndQuery(connectorSpecRequest.getPath(), "generate spec", (connection, path) -> {
      ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
      DatabaseMetaData metaData = connection.getMetaData();
      Map<String, String> properties = new HashMap<>();
      properties.put(DBBaseConfig.NAME_USE_CONNECTION, "true");
      properties.put(DBBaseConfig.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
      String table = path.getTable();
      if (table != null) {
        String schema = path.getSchema();
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

  private Connection getConnection() {
    try {
      return DriverManager.getConnection(config.getConnectionString(), config.getAllConnectionArguments());
    } catch (SQLException e) {
      throw new IllegalArgumentException(String.format("Cannot connect to database via connection string : %s and " +
                                                         "arguments: %s. Make sure you have correct connection " +
                                                         "properties.",
                                                     config.getConnectionString(), config.getConnectionArguments()), e);
    }
  }

  private BrowseDetail listSchemas(Connection connection, @Nullable Integer limit)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getSchemas()) {
      int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_SCHEM);
        if (count >= countLimit) {
          break;
        }

        browseDetailBuilder.addEntity(
          BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_SCHEMA).canBrowse(true).build());
        count++;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail getTableDetail(Connection connection, @Nullable String schema, String table)
    throws SQLException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), schema, table, SUPPORTED_TYPES)) {
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

  private BrowseDetail listTables(Connection connection, @Nullable String schema,
                                  @Nullable Integer limit) throws SQLException {
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
    try (ResultSet resultSet = metaData.getTables(connection.getCatalog(), schema, null, SUPPORTED_TYPES)) {
      int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
      while (resultSet.next()) {
        String name = resultSet.getString(RESULTSET_COLUMN_TABLE_NAME);
        if (count >= countLimit) {
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

  private List<StructuredRecord> getTableData(Connection connection, @Nullable String schema,
                                              String table, int limit) throws SQLException {
    String query = schema == null ? String.format("SELECT * FROM %s", table) :
      String.format("SELECT * FROM %s.%s", schema, table);
    try (Statement statement = connection.createStatement()) {
      statement.setFetchSize(limit);
      try (ResultSet resultSet = statement.executeQuery(query)) {
        return parseResultSet(resultSet, limit);
      }
    }
  }

  private <T> T connectAndQuery(String pathStr, String operation, Query<T> query) throws IOException {
    try (Connection connection = getConnection()) {
      DBPath path = new DBPath(pathStr, connection.getMetaData().supportsSchemasInTableDefinitions());
      return query.run(connection, path);

    } catch (SQLException e) {
      throw new IOException(String.format("Failed to %s.", operation), e);
    }
  }
  @FunctionalInterface
  interface Query<T> {
    T run(Connection connection, DBPath path) throws SQLException;
  }

  private static List<StructuredRecord> parseResultSet(ResultSet resultSet, int limit) throws SQLException {
    List<StructuredRecord> result = new ArrayList<>();
    Schema schema = Schema.recordOf("output", DBUtils.getSchemaFields(resultSet, null, null, null));
    ResultSetMetaData meta = resultSet.getMetaData();
    int count = 0;
    while (resultSet.next() && count < limit) {
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      for (int i = 1; i <= meta.getColumnCount(); ++i) {
        String fieldName = meta.getColumnName(i);
        int sqlType = meta.getColumnType(i);
        int sqlPrecision = meta.getPrecision(i);
        int sqlScale = meta.getScale(i);
        Schema fieldSchema = schema.getField(fieldName).getSchema();
        Object value = DBUtils.transformValue(sqlType, sqlPrecision, sqlScale, resultSet, fieldName, fieldSchema);
        if (fieldSchema.isNullable()) {
          fieldSchema = fieldSchema.getNonNullable();
        }
        if (value instanceof Date) {
          recordBuilder.setDate(fieldName, ((Date) value).toLocalDate());
        } else if (value instanceof Time) {
          recordBuilder.setTime(fieldName, ((Time) value).toLocalTime());
        } else if (value instanceof Timestamp) {
          recordBuilder
            .setTimestamp(fieldName, ((Timestamp) value).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        } else if (value instanceof BigDecimal) {
          recordBuilder.setDecimal(fieldName, (BigDecimal) value);
        } else if (value instanceof String && fieldSchema.getLogicalType() == Schema.LogicalType.DATETIME) {
          //make sure value is in the right format for datetime
          try {
            recordBuilder.setDateTime(fieldName, LocalDateTime.parse((String) value));
          } catch (DateTimeParseException exception) {
            throw new UnexpectedFormatException(
              String.format("Datetime field '%s' with value '%s' is not in ISO-8601 format.", fieldName, value),
              exception);
          }
        } else {
          recordBuilder.set(fieldName, value);
        }
      }
      result.add(recordBuilder.build());
      count++;
    }
    return result;
  }
}
