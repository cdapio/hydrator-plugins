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

package co.cask.hydrator.plugin.teradata.test;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import co.cask.hydrator.plugin.DatabasePluginTestBase;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.teradata.batch.source.TeradataSource;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for ETL using databases.
 */
public class TeradataPluginTestRun extends DatabasePluginTestBase {

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBSource() throws Exception {
    String importQuery = "SELECT ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, " +
      "DECIMAL_COL, BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, BLOB_COL, CLOB_COL FROM \"my_table\"" +
      "WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";
    Plugin sourceConfig = new Plugin(
      "Teradata",
      ImmutableMap.<String, String>builder()
        .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
        .put(Properties.DB.TABLE_NAME, "my_table")
        .put(Properties.DB.IMPORT_QUERY, importQuery)
        .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
        .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
        .put("numSplits", "1")
        .build()
    );

    Plugin sinkConfig = new Plugin("Table", ImmutableMap.of(
      "name", "outputTable",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID"));

    ETLStage source = new ETLStage("dbSource2", sourceConfig);
    ETLStage sink = new ETLStage("tableSink2", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dbSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    // Using get to verify the rowkey
    Assert.assertEquals(17, outputTable.get(Bytes.toBytes(1)).getColumns().size());
    // In the second record, the 'decimal' column is null
    Assert.assertEquals(16, outputTable.get(Bytes.toBytes(2)).getColumns().size());
    // Scanner to verify number of rows
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Row row2 = scanner.next();
    Assert.assertNotNull(row1);
    Assert.assertNotNull(row2);
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals("user1", row1.getString("NAME"));
    Assert.assertEquals("user2", row2.getString("NAME"));
    Assert.assertEquals(124.45, row1.getDouble("SCORE"), 0.000001);
    Assert.assertEquals(125.45, row2.getDouble("SCORE"), 0.000001);
    Assert.assertEquals(false, row1.getBoolean("GRADUATED"));
    Assert.assertEquals(true, row2.getBoolean("GRADUATED"));
    Assert.assertNull(row1.get("NOT_IMPORTED"));
    Assert.assertNull(row2.get("NOT_IMPORTED"));
    // TODO: Reading from table as SHORT seems to be giving the wrong value.
    Assert.assertEquals(1, (int) row1.getInt("TINY"));
    Assert.assertEquals(2, (int) row2.getInt("TINY"));
    Assert.assertEquals(1, (int) row1.getInt("SMALL"));
    Assert.assertEquals(2, (int) row2.getInt("SMALL"));
    Assert.assertEquals(1, (long) row1.getLong("BIG"));
    Assert.assertEquals(2, (long) row2.getLong("BIG"));
    // TODO: Reading from table as FLOAT seems to be giving back the wrong value.
    Assert.assertEquals(124.45, row1.getDouble("FLOAT_COL"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("FLOAT_COL"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("REAL_COL"), 0.00001);
    Assert.assertEquals(125.45, row2.getDouble("REAL_COL"), 0.00001);
    Assert.assertEquals(124.45, row1.getDouble("NUMERIC_COL"), 0.000001);
    Assert.assertEquals(125.45, row2.getDouble("NUMERIC_COL"), 0.000001);
    Assert.assertEquals(124.45, row1.getDouble("DECIMAL_COL"), 0.000001);
    Assert.assertEquals(null, row2.get("DECIMAL_COL"));
    Assert.assertEquals(true, row1.getBoolean("BIT_COL"));
    Assert.assertEquals(false, row2.getBoolean("BIT_COL"));
    // Verify time columns
    java.util.Date date = new java.util.Date(CURRENT_TS);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    long expectedDateTimestamp = Date.valueOf(sdf.format(date)).getTime();
    sdf = new SimpleDateFormat("H:mm:ss");
    long expectedTimeTimestamp = Time.valueOf(sdf.format(date)).getTime();
    Assert.assertEquals(expectedDateTimestamp, (long) row1.getLong("DATE_COL"));
    Assert.assertEquals(expectedDateTimestamp, (long) row2.getLong("DATE_COL"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row1.getLong("TIME_COL"));
    Assert.assertEquals(expectedTimeTimestamp, (long) row2.getLong("TIME_COL"));
    Assert.assertEquals(CURRENT_TS, (long) row1.getLong("TIMESTAMP_COL"));
    Assert.assertEquals(CURRENT_TS, (long) row2.getLong("TIMESTAMP_COL"));
    // verify binary columns
    Assert.assertEquals("user1", Bytes.toString(row1.get("BINARY_COL"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BINARY_COL"), 0, 5));
    Assert.assertEquals("user1", Bytes.toString(row1.get("BLOB_COL"), 0, 5));
    Assert.assertEquals("user2", Bytes.toString(row2.get("BLOB_COL"), 0, 5));
    Assert.assertEquals(CLOB_DATA, Bytes.toString(row1.get("CLOB_COL"), 0, CLOB_DATA.length()));
    Assert.assertEquals(CLOB_DATA, Bytes.toString(row2.get("CLOB_COL"), 0, CLOB_DATA.length()));
  }

  @Test
  public void testDBSourceWithLowerCaseColNames() throws Exception {
    // all lower case since we are going to set db column name case to be lower
    Schema schema = Schema.recordOf("student",
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    String importQuery = "SELECT ID, NAME FROM \"my_table\" WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";
    Plugin sourceConfig = new Plugin("Teradata", ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
      .put(Properties.DB.TABLE_NAME, "my_table")
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .put(Properties.DB.COLUMN_NAME_CASE, "lower")
      .build()
    );

    ETLStage source = new ETLStage("dbSource1", sourceConfig);
    Plugin sinkConfig = new Plugin("Table", ImmutableMap.of(
      "name", "outputTable1",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      // smaller case since we have set the db data's column case to be lower
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "id"));
    ETLStage sink = new ETLStage("tableSink1", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "teradataSourceTest");
    ApplicationManager appManager = TestBase.deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    List<RunRecord> runRecords = mrManager.getHistory();
    Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecords.get(0).getStatus());

    // records should be written
    DataSetManager<Table> outputManager = getDataset("outputTable1");
    Table outputTable = outputManager.get();
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Row row2 = scanner.next();
    Assert.assertNotNull(row1);
    Assert.assertNotNull(row2);
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals("user1", row1.getString("name"));
    Assert.assertEquals("user2", row2.getString("name"));
    Assert.assertEquals(1, Bytes.toInt(row1.getRow()));
    Assert.assertEquals(2, Bytes.toInt(row2.getRow()));
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    Schema schema = Schema.recordOf("student",
                                    Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)));

    // have the same data in both tables ('\"my_table\"' and '\"your_table\"'), and select the ID and NAME fields from
    // separate tables
    String importQuery = "SELECT \"my_table\".ID, \"your_table\".NAME FROM \"my_table\", \"your_table\"" +
      "WHERE \"my_table\".ID < 3 and \"my_table\".ID = \"your_table\".ID and $CONDITIONS";
    String boundingQuery = "SELECT MIN(MIN(\"my_table\".ID), MIN(\"your_table\".ID)), " +
      "MAX(MAX(\"my_table\".ID), MAX(\"your_table\".ID))";
    String splitBy = "\"my_table\".ID";
    Plugin sourceConfig = new Plugin("Teradata", ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
      .put(Properties.DB.TABLE_NAME, "my_table")
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .build()
    );

    Plugin sinkConfig = new Plugin("Table", ImmutableMap.of(
      "name", "outputTable1",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID"));

    ETLStage source = new ETLStage("dbSource3", sourceConfig);
    ETLStage sink = new ETLStage("tableSink3", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dbSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    List<RunRecord> runRecords = mrManager.getHistory();
    Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecords.get(0).getStatus());

    // records should be written
    DataSetManager<Table> outputManager = getDataset("outputTable1");
    Table outputTable = outputManager.get();
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Row row2 = scanner.next();
    Assert.assertNotNull(row1);
    Assert.assertNotNull(row2);
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals("user1", row1.getString("NAME"));
    Assert.assertEquals("user2", row2.getString("NAME"));
    Assert.assertEquals(1, Bytes.toInt(row1.getRow()));
    Assert.assertEquals(2, Bytes.toInt(row2.getRow()));
  }

  @Test
  public void testUserNamePasswordCombinations() throws Exception {
    String importQuery = "SELECT * FROM \"my_table\" WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from \"my_table\"";
    String splitBy = "ID";

    Plugin tableConfig = new Plugin("Table", ImmutableMap.of(
      "name", "outputTable",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID"));

    Map<String, String> baseSourceProps = ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
      .put(Properties.DB.TABLE_NAME, "my_table")
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
      .build();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dbTest");

    // null user name, null password. Should succeed.
    // as source
    Plugin dbConfig = new Plugin("Teradata", baseSourceProps);
    ETLStage table = new ETLStage("uniqueTableSink" , tableConfig);
    ETLStage database = new ETLStage("databaseSource" , dbConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(database)
      .addSink(table)
      .addConnection(database.getName(), table.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);

    // non null user name, null password. Should fail.
    // as source
    Map<String, String> noPassword = new HashMap<>(baseSourceProps);
    noPassword.put(Properties.DB.USER, "emptyPwdUser");
    database = new ETLStage("databaseSource", new Plugin("Teradata", noPassword));
    etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(database)
      .addSink(table)
      .addConnection(database.getName(), table.getName())
      .build();
    assertDeploymentFailure(
      appId, etlConfig, "Deploying DB Source with non-null username but null password should have failed.");

    // null user name, non-null password. Should fail.
    // as source
    Map<String, String> noUser = new HashMap<>(baseSourceProps);
    noUser.put(Properties.DB.PASSWORD, "password");
    database = new ETLStage("databaseSource", new Plugin("Teradata", noUser));
    etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(database)
      .addSink(table)
      .addConnection(database.getName(), table.getName())
      .build();
    assertDeploymentFailure(
      appId, etlConfig, "Deploying DB Source with null username but non-null password should have failed.");

    // non-null username, non-null, but empty password. Should succeed.
    // as source
    Map<String, String> emptyPassword = new HashMap<>(baseSourceProps);
    emptyPassword.put(Properties.DB.USER, "emptyPwdUser");
    emptyPassword.put(Properties.DB.PASSWORD, "");
    database = new ETLStage("databaseSource", new Plugin("Teradata", emptyPassword));
    etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(database)
      .addSink(table)
      .addConnection(database.getName(), table.getName())
      .build();
    appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);
  }

  @Test
  public void testNonExistentDBTable() throws Exception {
    // source
    String importQuery = "SELECT ID, NAME FROM dummy WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) FROM dummy";
    String splitBy = "ID";
    //TODO: Also test for bad connection:
    Plugin tableConfig = new Plugin("Table", ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, "table",
      Properties.Table.PROPERTY_SCHEMA, schema.toString(),
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID"));
    Plugin sourceBadNameConfig = new Plugin("Teradata", ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
      .put(Properties.DB.TABLE_NAME, "dummy")
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
      .build());
    ETLStage table = new ETLStage("tableName", tableConfig);
    ETLStage sourceBadName = new ETLStage("sourceBadName", sourceBadNameConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(sourceBadName)
      .addSink(table)
      .addConnection(sourceBadName.getName(), table.getName())
      .build();
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dbSourceNonExistingTest");
    assertRuntimeFailure(appId, etlConfig, "ETL Application with DB Source should have failed because of a " +
      "non-existent source table.");

    // Bad connection
    String badConnection = String.format("jdbc:hsqldb:hsql://localhost/%sWRONG", getDatabase());
    Plugin sourceBadConnConfig = new Plugin("Teradata", ImmutableMap.<String, String>builder()
      .put(Properties.DB.CONNECTION_STRING, badConnection)
      .put(Properties.DB.TABLE_NAME, "dummy")
      .put(Properties.DB.IMPORT_QUERY, importQuery)
      .put(TeradataSource.TeradataSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
      .put(TeradataSource.TeradataSourceConfig.SPLIT_BY, splitBy)
      .build());
    ETLStage sourceBadConn = new ETLStage("sourceBadConn", sourceBadConnConfig);
    etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(sourceBadConn)
      .addSink(table)
      .addConnection(sourceBadConn.getName(), table.getName())
      .build();
    assertRuntimeFailure(appId, etlConfig, "ETL Application with DB Source should have failed because of a " +
      "non-existent source database.");
  }
}
