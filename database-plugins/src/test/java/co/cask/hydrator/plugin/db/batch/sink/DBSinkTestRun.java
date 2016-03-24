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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
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
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "dbSinkTest");
    ApplicationManager appManager = TestBase.deployApplication(appId, appRequest);

    createInputData();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    List<RunRecord> runRecords = mrManager.getHistory();
    Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecords.get(0).getStatus());

    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("SELECT * FROM \"MY_DEST_TABLE\"");
    ResultSet resultSet = stmt.getResultSet();
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("user1", resultSet.getString("NAME"));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("user2", resultSet.getString("NAME"));
    Assert.assertFalse(resultSet.next());
    resultSet.close();
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
}
