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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.validation.SimpleFailureCollector;
import io.cdap.plugin.db.batch.source.DBSource;
import io.cdap.plugin.db.common.DBBaseConfig;
import io.cdap.plugin.db.common.DBDifferenceUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for Database Connector, it will only be run when below properties are provided:
 * -Dusername -- the username used to connect to database
 * -Dpassword -- the password used to connect to database
 * -Dconnection.string -- the JDBC connection string used to connect to database
 * -Ddriver.class -- the fully specified class name of the JDBC drvier class
 * -Ddatabase.name -- the database name
 * -Dschema.name -- the schema name, optional for those DBs that don't support schema
 * -Dtable.name -- the table name
 * -Dconnection.arguments -- the additional connection arguments, optional
 */
public class DBConnectorTest {
  private static final Set<String> SUPPORTED_TYPES = new HashSet<>(Arrays.asList(DBConnector.SUPPORTED_TYPES));
  private static final String JDBC_PLUGIN_NAME = "jdbc_plugin";
  private static String username;
  private static String password;
  private static String connectionString;
  private static String connectionArguments;
  private static Class driverClass;
  private static String table;
  private static String database;
  private static String schema;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    username = System.getProperty("username");
    Assume.assumeFalse(String.format(messageTemplate, "username"), username == null);

    password = System.getProperty("password");
    Assume.assumeFalse(String.format(messageTemplate, "password"), password == null);

    connectionString = System.getProperty("connection.string");
    Assume.assumeFalse(String.format(messageTemplate, "connection string"), connectionString == null);

    String driver = System.getProperty("driver.class");
    Assume.assumeFalse(String.format(messageTemplate, "JDBC driver class"), driver == null);
    driverClass = DBConnectorTest.class.getClassLoader().loadClass(driver);

    database = System.getProperty("database.name");
    Assume.assumeFalse(String.format(messageTemplate, "database name"), database == null);

    schema = System.getProperty("schema.name");

    table = System.getProperty("table.name");
    Assume.assumeFalse(String.format(messageTemplate, "table name"), table == null);

    connectionArguments = System.getProperty("connection.arguments");
  }


  @Test
  public void test() throws IOException {
    DBConnector connector = new DBConnector(
      new DBConnectorConfig(username, password, JDBC_PLUGIN_NAME, connectionString, connectionArguments));
    PluginConfigurer configurer = Mockito.mock(PluginConfigurer.class);
    Mockito.when(configurer.usePluginClass(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
      Mockito.any(PluginProperties.class))).thenReturn(driverClass);
    testTest(connector);
    testBrowse(connector);
    testSample(connector);
    testGenerateSpec(connector);
  }

  private void testSample(DBConnector connector) throws IOException {
    List<StructuredRecord> sample = connector.sample(
      SampleRequest.builder(1).setPath(schema == null ? database + "/" + table : database + "/" + schema + "/" + table)
        .build());
    Assert.assertEquals(1, sample.size());
    StructuredRecord record = sample.get(0);
    Schema tableSchema = record.getSchema();
    Assert.assertNotNull(tableSchema);
    for (Schema.Field field : tableSchema.getFields()) {
      Assert.assertNotNull(field.getSchema());
      Assert.assertTrue(record.get(field.getName()) != null || field.getSchema().isNullable());
    }

    //invalid path
    Assert.assertThrows(IllegalArgumentException.class,
      () -> connector.sample(SampleRequest.builder(1).setPath(schema == null ? "a/b/c" : "a/b/c/d").build()));

    //sample database
    Assert.assertThrows(IllegalArgumentException.class,
      () -> connector.sample(SampleRequest.builder(1).setPath(database).build()));

    if (schema != null) {
      //sample tableSchema
      Assert.assertThrows(IllegalArgumentException.class,
        () -> connector.sample(SampleRequest.builder(1).setPath(database + "/" + schema).build()));
    }
  }

  private void testBrowse(DBConnector connector) throws IOException {
    // browse DB server
    BrowseDetail detail = connector.browse(BrowseRequest.builder("/").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertEquals(DBConnector.ENTITY_TYPE_DATABASE, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertFalse(entity.canSample());
    }

    if (schema != null) {
      // browse database to list schema
      detail = connector.browse(BrowseRequest.builder(database).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertEquals(DBConnector.ENTITY_TYPE_SCHEMA, entity.getType());
        Assert.assertTrue(entity.canBrowse());
        Assert.assertFalse(entity.canSample());
      }
      // browse schema to list tables
      detail = connector.browse(BrowseRequest.builder(database + "/" + schema).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertTrue(SUPPORTED_TYPES.contains(entity.getType()));
        Assert.assertFalse(entity.canBrowse());
        Assert.assertTrue(entity.canSample());
      }

    } else {

      // browse database to list tables
      detail = connector.browse(BrowseRequest.builder(database).build());
      Assert.assertTrue(detail.getTotalCount() > 0);
      Assert.assertTrue(detail.getEntities().size() > 0);
      for (BrowseEntity entity : detail.getEntities()) {
        System.out.println(entity.getType() + " : " + entity.getName());
        Assert.assertTrue(SUPPORTED_TYPES.contains(entity.getType()));
        Assert.assertFalse(entity.canBrowse());
        Assert.assertTrue(entity.canSample());
      }
    }

    // browse table
    detail = connector.browse(
      BrowseRequest.builder(schema == null ? database + "/" + table : database + "/" + schema + "/" + table).build());
    Assert.assertEquals(1, detail.getTotalCount());
    Assert.assertEquals(1, detail.getEntities().size());
    for (BrowseEntity entity : detail.getEntities()) {
      System.out.println(entity.getType() + " : " + entity.getName());
      Assert.assertTrue(SUPPORTED_TYPES.contains(entity.getType()));
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // invalid path
    Assert.assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder(schema == null ? "a/b/c" : "a/b/c/d").build()));

    // not existing database
    Assert.assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder("/notexisting").build()));

    // not existing schema or table
    Assert.assertThrows(IllegalArgumentException.class,
      () -> connector.browse(BrowseRequest.builder(database + "/notexisting").build()));

    if (schema != null) {
      // not existing table
      Assert.assertThrows(IllegalArgumentException.class,
        () -> connector.browse(BrowseRequest.builder(database + "/" + schema + "/notexisting").build()));
    }
  }

  private void testTest(DBConnector connector) {
    SimpleFailureCollector collector = new SimpleFailureCollector();
    connector.test(collector);
    ValidationException validationException = collector.getOrThrowException();
    Assert.assertTrue(validationException.getFailures().isEmpty());
  }

  private void testGenerateSpec(DBConnector connector) throws IOException {
    ConnectorSpec connectorSpec = connector.generateSpec(ConnectorSpecRequest.builder()
      .setPath(schema == null ? database + "/" + table : database + "/" + schema + "/" + table)
      .setConnection("${conn(connection-id)}").build());
    Schema tableSchema = connectorSpec.getSchema();
    for (Schema.Field field : tableSchema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(DBSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());

    Map<String, String> properties = pluginSpec.getProperties();
    if (DBDifferenceUtils.replaceDatabase(connectionString, database).equals(connectionString)) {
      Assert.assertEquals("true", properties.get(DBBaseConfig.NAME_USE_CONNECTION));
      Assert.assertEquals("${conn(connection-id)}", properties.get(DBBaseConfig.NAME_CONNECTION));
    } else {
      Assert.assertEquals("false", properties.get(DBBaseConfig.NAME_USE_CONNECTION));
      Assert.assertEquals(password, properties.get(DBConnectorConfig.PASSWORD));
      Assert.assertEquals(username, properties.get(DBConnectorConfig.USER));
      Assert.assertEquals(connectionArguments, properties.get(DBConnectorConfig.CONNECTION_ARGUMENTS));
      Assert.assertEquals(JDBC_PLUGIN_NAME, properties.get(DBConnectorConfig.JDBC_PLUGIN_NAME));
    }

    Assert.assertEquals(schema == null ? String.format("SELECT * FROM %s;", table) :
        String.format("SELECT * FROM %s.%s;", schema, table),
      properties.get(DBSource.DBSourceConfig.IMPORT_QUERY));
    properties.put("1", properties.get(DBSource.DBSourceConfig.NUM_SPLITS));
  }
}
