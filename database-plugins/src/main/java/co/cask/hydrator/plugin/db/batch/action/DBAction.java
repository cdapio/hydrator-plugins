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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.plugin.ConnectionConfig;
import co.cask.hydrator.plugin.DBManager;
import com.ibatis.common.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Action that runs a db script
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("DBAction")
@Description("Action that runs a db script")
public class DBAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DBAction.class);
  private static final String JDBC_PLUGIN_ID = "driver";
  private final DBActionConfig config;

  public DBAction(DBActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {

    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    DBManager dbManager = new DBManager(config);

    try {
      dbManager.ensureJDBCDriverIsAvailable(driverClass);

      try (Connection connection = getConnection()) {
        if (!config.enableAutoCommit) {
          connection.setAutoCommit(false);
        }
        ScriptRunner runner = new ScriptRunner(connection, config.enableAutoCommit, true);
        runner.runScript(new BufferedReader(new FileReader(config.scriptPath)));
        if (!config.enableAutoCommit) {
          connection.commit();
        }
      }
    } catch (Exception e) {
      LOG.error("Error running script {}.", config.scriptPath, e);
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
    DBManager dbManager = new DBManager(config);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, JDBC_PLUGIN_ID);
  }

  /**
   * Config for DBAction
   */
  public class DBActionConfig extends ConnectionConfig {
    @Description("The file path of the script to run.")
    @Macro
    public String scriptPath;

    public DBActionConfig() {
      super();

    }
  }
}
