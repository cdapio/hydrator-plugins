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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.plugin.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Runs a query after a pipeline run.
 */
@SuppressWarnings("ConstantConditions")
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("DatabaseQuery")
@Description("Runs a query after a pipeline run.")
public class QueryAction extends PostAction {
  private static final Logger LOG = LoggerFactory.getLogger(QueryAction.class);
  private static final String JDBC_PLUGIN_ID = "driver";
  private final QueryConfig config;

  public QueryAction(QueryConfig config) {
    this.config = config;
  }

  @Override
  public void run(BatchActionContext batchContext) throws Exception {
    if (!config.shouldRun(batchContext)) {
      return;
    }
    config.substituteMacros(batchContext);

    Class<? extends Driver> driverClass = batchContext.loadPluginClass(JDBC_PLUGIN_ID);
    DBManager dbManager = new DBManager(config);

    try {
      dbManager.ensureJDBCDriverIsAvailable(driverClass);

      try (Connection connection = getConnection()) {
        if (!config.enableAutoCommit) {
          connection.setAutoCommit(false);
        }
        try (Statement statement = connection.createStatement()) {
          statement.execute(config.query);
          if (!config.enableAutoCommit) {
            connection.commit();
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error running query {}.", config.query, e);
    } finally {
      dbManager.destroy();
    }
  }

  private Connection getConnection() throws SQLException {
    if (config.user == null) {
      return DriverManager.getConnection(config.connectionString);
    } else {
      return DriverManager.getConnection(config.connectionString, config.user, config.password);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate();
    DBManager dbManager = new DBManager(config);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, JDBC_PLUGIN_ID);
  }
}
