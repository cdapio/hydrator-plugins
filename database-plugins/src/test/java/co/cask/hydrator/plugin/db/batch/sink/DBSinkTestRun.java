/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.DBConfig;
import co.cask.hydrator.plugin.DatabasePluginTestBase;
import co.cask.hydrator.plugin.db.batch.source.DBSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test for ETL using databases.
 */
public class DBSinkTestRun extends DatabasePluginTestBase {

  @Test
  public void testDBSink() throws Exception {
    String inputDatasetName = "input-dbsinktest";

    String cols = "ID, NAME, SCORE, GRADUATED, TINY, SMALL, BIG, FLOAT_COL, REAL_COL, NUMERIC_COL, DECIMAL_COL, " +
      "BIT_COL, DATE_COL, TIME_COL, TIMESTAMP_COL, BINARY_COL, BLOB_COL, CLOB_COL";
    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName);
    ETLPlugin sinkConfig = new ETLPlugin("Database",
                                         BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of(DBConfig.CONNECTION_STRING, getConnectionURL(),
                                                         DBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE",
                                                         DBSink.DBSinkConfig.COLUMNS, "${col-macro}",
                                                         DBConfig.JDBC_PLUGIN_NAME, "hypersql",
                                                         Constants.Reference.REFERENCE_NAME, "DBTest"),
                                         null);
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, "testDBSink");
    createInputData(inputDatasetName);

    runETLOnce(appManager, Collections.singletonMap("col-macro", cols));

    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("SELECT * FROM \"MY_DEST_TABLE\"");
      Set<String> users = new HashSet<>();
      try (ResultSet resultSet = stmt.getResultSet()) {
        Assert.assertTrue(resultSet.next());
        users.add(resultSet.getString("NAME"));
        Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
        Assert.assertEquals(new Time(CURRENT_TS).toString(), resultSet.getTime("TIME_COL").toString());
        Assert.assertEquals(new Timestamp(CURRENT_TS).toString(),
                            resultSet.getTimestamp("TIMESTAMP_COL").toString());
        Assert.assertTrue(resultSet.next());
        users.add(resultSet.getString("NAME"));
        Assert.assertFalse(resultSet.next());
        Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);
      }
    }
  }

  @Test
  public void testNullFields() throws Exception {
    prepareInputAndOutputTables();
    String importQuery = "SELECT A, B, C FROM INPUT WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(A),MAX(A) from INPUT";
    String splitBy = "A";
    ETLPlugin sourceConfig = new ETLPlugin(
      "Database",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(DBConfig.CONNECTION_STRING, getConnectionURL())
        .put(DBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(DBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(DBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(DBConfig.JDBC_PLUGIN_NAME, "hypersql")
        .put(Constants.Reference.REFERENCE_NAME, "DBTestSource")
        .build(),
      null
    );
    ETLPlugin sinkConfig = new ETLPlugin(
      "Database",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(
        DBConfig.CONNECTION_STRING, getConnectionURL(),
        DBSink.DBSinkConfig.TABLE_NAME, "OUTPUT",
        DBSink.DBSinkConfig.COLUMNS, "A, B, C",
        DBConfig.JDBC_PLUGIN_NAME, "hypersql",
        Constants.Reference.REFERENCE_NAME, "DBTestSink"),
      null
    );
    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, "testNullFields");
    // if nulls are not handled correctly, the MR program will fail with an NPE
    runETLOnce(appManager);
  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("GRADUATED", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("TINY", Schema.of(Schema.Type.INT)),
      Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("REAL_COL", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("NUMERIC_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("DECIMAL_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("BIT_COL", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("CLOB_COL", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      inputRecords.add(StructuredRecord.builder(schema)
                         .set("ID", i)
                         .set("NAME", name)
                         .set("SCORE", 3.451f)
                         .set("GRADUATED", (i % 2 == 0))
                         .set("TINY", i + 1)
                         .set("SMALL", i + 2)
                         .set("BIG", 3456987L)
                         .set("FLOAT_COL", 3.456f)
                         .set("REAL_COL", 3.457f)
                         .set("NUMERIC_COL", 3.458d)
                         .set("DECIMAL_COL", 3.459d)
                         .set("BIT_COL", (i % 2 == 1))
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTime("TIME_COL", localDateTime.toLocalTime())
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
                         .set("BLOB_COL", name.getBytes(Charsets.UTF_8))
                         .set("CLOB_COL", CLOB_DATA)
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
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
