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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.db.AbstractDBConnector;
import io.cdap.plugin.common.db.DBConnectorPath;
import io.cdap.plugin.common.db.DBPath;
import io.cdap.plugin.common.db.DBUtils;
import io.cdap.plugin.common.db.dbrecordreader.RecordReader;
import io.cdap.plugin.db.batch.sink.DBSink;
import io.cdap.plugin.db.batch.source.DBSource;
import io.cdap.plugin.db.common.DBBaseConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
@Description("Connection to access data in relational databases using JDBC.")
@Category("Database")
public class DBConnector extends AbstractDBConnector<DBConnectorConfig> implements DirectConnector {
  public static final String NAME = "Database";

  private final DBConnectorConfig config;

  public DBConnector(DBConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected DBConnectorPath getDBConnectorPath(String path) throws IOException {
    try {
      return new DBPath(path, getConnection().getMetaData().supportsSchemasInTableDefinitions());
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to parse the path %s for the connector", path), e);
    }
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext context, SampleRequest sampleRequest) throws IOException {
    try (Connection connection = getConnection()) {
      DBPath path = new DBPath(sampleRequest.getPath(), connection.getMetaData().supportsSchemasInTableDefinitions());
      String table = path.getTable();
      if (table == null) {
        throw new IllegalArgumentException("Path should contain table name.");
      }
      String schema = path.getSchema();
      return getTableData(connection, schema, table, sampleRequest.getLimit());

    } catch (SQLException e) {
      throw new IOException("Failed to sample.", e);
    }
  }

  protected void setConnectorSpec(ConnectorSpecRequest request, DBConnectorPath path,
                                  ConnectorSpec.Builder builder) {
    Map<String, String> properties = new HashMap<>();
    properties.put(DBBaseConfig.NAME_USE_CONNECTION, "true");
    properties.put(DBBaseConfig.NAME_CONNECTION, request.getConnectionWithMacro());
    properties.put(DBBaseConfig.JDBC_PLUGIN_TYPE, DBUtils.PLUGIN_TYPE_JDBC);
    if (path.getTable() != null) {
      properties.put(Constants.Reference.REFERENCE_NAME, path.getTable());
    }
    builder
      .addRelatedPlugin(new PluginSpec(DBSource.NAME, BatchSource.PLUGIN_TYPE, properties))
      .addRelatedPlugin(new PluginSpec(DBSink.NAME, BatchSink.PLUGIN_TYPE, properties));

    String table = path.getTable();
    if (table == null) {
      return;
    }

    String schema = path.getSchema();
    properties.put(DBSource.DBSourceConfig.IMPORT_QUERY,
                   schema == null ? String.format("SELECT * FROM %s;", table) :
                     String.format("SELECT * FROM %s.%s;", schema, table));
    properties.put(DBSource.DBSourceConfig.NUM_SPLITS, "1");
  }

  private List<StructuredRecord> getTableData(Connection connection, @Nullable String schema,
                                              String table, int limit) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    validateSchema(connection.getCatalog(), schema, connection);
    String query = schema == null ? String.format("SELECT * FROM %s", table) :
      String.format("SELECT * FROM %s.%s", schema, table);
    try (Statement statement = connection.createStatement()) {
      statement.setFetchSize(limit);
      try (ResultSet resultSet = statement.executeQuery(query)) {
        String dbProductName = connection.getMetaData().getDatabaseProductName();
        return parseResultSet(dbProductName, resultSet, limit);
      }
    }
  }

  private static List<StructuredRecord> parseResultSet(String dbProductName,
                                                       ResultSet resultSet, int limit) throws SQLException {
    List<StructuredRecord> result = new ArrayList<>();
    Schema schema = Schema.recordOf("output",
            DBUtils.getSchemaReader(dbProductName, BatchSource.PLUGIN_TYPE, null)
                    .getSchemaFields(resultSet, null, null));
    ResultSetMetaData meta = resultSet.getMetaData();
    int count = 0;

    RecordReader recordReader = DBUtils.getRecordReaderHelper(dbProductName);
    while (resultSet.next() && count < limit) {
      result.add(recordReader.getRecordBuilder(resultSet, schema).build());
      count++;
    }
    return result;
  }
}
