/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.db.batch.action;

import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.DatabasePluginTestBase;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 */
public class DBQueryActionTestRun extends DatabasePluginTestBase {

  @Test
  public void testAction() throws Exception {
    // create a table that the action will truncate at the end of the run
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("create table \"actionTest\" (x int)");
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("insert into \"actionTest\" values (1)");
      }
    }

    ETLStage source = new ETLStage("source",
                                   new Plugin("Table",
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, "dummyTable",
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID",
                                                Properties.Table.PROPERTY_SCHEMA, schema.toString())));
    ETLStage sink = new ETLStage("sink",
                                 new Plugin("Table",
                                            ImmutableMap.of(
                                              "name", "dummyTable",
                                              Properties.Table.PROPERTY_SCHEMA, schema.toString(),
                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ID")));
    ETLStage action = new ETLStage("action",
                                   new Plugin("DatabaseQuery",
                                              ImmutableMap.<String, String>builder()
                                                .put("connectionString", getConnectionURL())
                                                .put("jdbcPluginName", "hypersql")
                                                .put("query", "truncate table \"actionTest\"")
                                                .build()));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addAction(action)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "actionTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet results = statement.executeQuery("select * from \"actionTest\"")) {
          Assert.assertFalse(results.next());
        }
      }
    }
  }
}
