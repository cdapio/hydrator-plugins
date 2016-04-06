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

package co.cask.hydrator.plugin.db.batch.sink;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.DatabasePluginTestBase;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.db.batch.source.DBSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for ETL using databases.
 */
public class DBSinkTestRun extends DatabasePluginTestBase {

  @Test
  public void testDBSink() throws Exception {
    String cols = "ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, DECIMAL_COL, " +
      "BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, BLOB_COL, CLOB_COL";
    Plugin sourceConfig = new Plugin("Table",
                                     ImmutableMap.of(
                                       Properties.BatchReadableWritable.NAME, "DBInputTable",
                                       Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID",
                                       Properties.Table.PROPERTY_SCHEMA, schema.toString(),
                                       Properties.DB.USER, "emptyPwdUser"));
    Plugin sinkConfig = new Plugin("Database",
                                   ImmutableMap.of(Properties.DB.CONNECTION_STRING, getConnectionURL(),
                                                   Properties.DB.TABLE_NAME, "MY_DEST_TABLE",
                                                   Properties.DB.COLUMNS, cols,
                                                   Properties.DB.JDBC_PLUGIN_NAME, "hypersql"
                                   ));
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig);

    createInputData();

    runETLOnce(appManager);

    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("SELECT * FROM \"MY_DEST_TABLE\"");
      try (ResultSet resultSet = stmt.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("user1", resultSet.getString("NAME"));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("user2", resultSet.getString("NAME"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNullFields() throws Exception {
    prepareInputAndOutputTables();
    String importQuery = "SELECT A, B, C FROM INPUT WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(A),MAX(A) from INPUT";
    String splitBy = "A";
    Plugin sourceConfig = new Plugin(
      "Database",
      ImmutableMap.<String, String>builder()
        .put(Properties.DB.CONNECTION_STRING, getConnectionURL())
        .put(Properties.DB.TABLE_NAME, "INPUT")
        .put(Properties.DB.IMPORT_QUERY, importQuery)
        .put(DBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(DBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
        .build()
    );
    Plugin sinkConfig = new Plugin(
      "Database",
      ImmutableMap.of(
        Properties.DB.CONNECTION_STRING, getConnectionURL(),
        Properties.DB.TABLE_NAME, "OUTPUT",
        Properties.DB.COLUMNS, "A, B, C",
        Properties.DB.JDBC_PLUGIN_NAME, "hypersql")
    );
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig);
    // if nulls are not handled correctly, the MR program will fail with an NPE
    runETLOnce(appManager);
  }

  private void createInputData() throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("DBInputTable");
    Table inputTable = inputManager.get();
    for (int i = 1; i <= 2; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      String name = "user" + i;
      put.add("ID", i);
      put.add("NAME", name);
      put.add("SCORE", 3.451);
      put.add("GRADUATED", (i % 2 == 0));
      put.add("TINY", i + 1);
      put.add("SMALL", i + 2);
      put.add("BIG", 3456987L);
      put.add("FLOAT_COL", 3.456f);
      put.add("REAL_COL", 3.457f);
      put.add("NUMERIC_COL", 3.458);
      put.add("DECIMAL_COL", 3.459);
      put.add("BIT_COL", (i % 2 == 1));
      put.add("DATE_COL", CURRENT_TS);
      put.add("TIME_COL", CURRENT_TS);
      put.add("TIMESTAMP_COL", CURRENT_TS);
      put.add("BINARY_COL", name.getBytes(Charsets.UTF_8));
      put.add("BLOB_COL", name.getBytes(Charsets.UTF_8));
      put.add("CLOB_COL", CLOB_DATA);
      inputTable.put(put);
      inputManager.flush();
    }
  }

  private void prepareInputAndOutputTables() throws SQLException {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("create table INPUT (a float, b double, c varchar(20))");
      try (PreparedStatement pStmt = conn.prepareStatement("insert into INPUT values(?,?,?)")) {
        for (int i = 1; i <= 5; i++) {
          if (i % 2 == 0) {
            pStmt.setNull(1, 6);
            pStmt.setDouble(2, 5.44342321332d);
          } else {
            pStmt.setFloat(1, 4.3f);
            pStmt.setNull(2, 8);
          }
          pStmt.setString(3, "input" + i);
          pStmt.addBatch();
        }
        pStmt.executeBatch();
      }
      // create output table
      stmt.execute("create table OUTPUT (a float, b double, c varchar(20))");
    }
  }
}
