/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.plugin.DBManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Runs a query after a pipeline run.
 */
@SuppressWarnings("ConstantConditions")
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("VerticaBulkLoadAction")
@Description("Runs a query after a pipeline run.")
public class VerticaBulkLoadAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(VerticaBulkLoadAction.class);
  private static final String JDBC_PLUGIN_ID = "driver";
  private final VerticaBulkLoadConfig config;

  public VerticaBulkLoadAction(VerticaBulkLoadConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    Path path = new Path(config.path);
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    DBManager dbManager = new DBManager(config);


    try {
      dbManager.ensureJDBCDriverIsAvailable(driverClass);

      try (Connection connection = getConnection()) {
        if (!config.enableAutoCommit) {
          connection.setAutoCommit(false);
        }
        // run Copy statement
//        VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection) conn, copyQuery);
      }
    } catch (Exception e) {
      LOG.error("Error running query {}.", config.copyStatement, e);
    } finally {
      dbManager.destroy();
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    DBManager dbManager = new DBManager(config);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, JDBC_PLUGIN_ID);
  }

  private Connection getConnection() throws SQLException {
    if (config.user == null) {
      return DriverManager.getConnection(config.connectionString);
    } else {
      return DriverManager.getConnection(config.connectionString, config.user, config.password);
    }
  }
}
