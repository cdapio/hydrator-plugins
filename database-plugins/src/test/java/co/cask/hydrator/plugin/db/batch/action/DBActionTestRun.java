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

import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.DatabasePluginTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Test for DBAction Plugin
 */
public class DBActionTestRun extends DatabasePluginTestBase {

  @Test
  public void testDBAction() throws Exception {
    // create a table that the action will truncate at the end of the run
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("create table \"dbActionTest\" (x int, day varchar(10))");
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("insert into \"dbActionTest\" values (1, '1970-01-01')");
      }
    }

    ETLStage source = new ETLStage("source", MockSource.getPlugin("actionInput"));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin("actionOutput"));
    ETLStage action = new ETLStage("action", new ETLPlugin(
      "Database",
      Action.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("connectionString", getConnectionURL())
        .put("jdbcPluginName", "hypersql")
        .put("query", "delete from \"dbActionTest\" where day = '${logicalStartTime(yyyy-MM-dd,0m,UTC)}'")
        .build(),
      null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(action)
      .addConnection(sink.getName(), action.getName())
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "actionTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("logical.start.time", "0"));
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet results = statement.executeQuery("select * from \"dbActionTest\"")) {
          Assert.assertFalse(results.next());
        }
      }
    }
  }
}
