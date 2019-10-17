/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Destroyable;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * Class to manage common database operations for Database source and sink plugins.
 */
public class DBManager implements Destroyable {
  private static final Logger LOG = LoggerFactory.getLogger(DBManager.class);
  private final ConnectionConfig config;
  private DriverCleanup driverCleanup;

  public DBManager(ConnectionConfig config) {
    this.config = config;
  }

  @Nullable
  public Class<? extends Driver> validateJDBCPluginPipeline(PipelineConfigurer pipelineConfigurer,
                                                            String jdbcPluginId, FailureCollector collector) {
    if (!config.containsMacro(DBConfig.USER) && !config.containsMacro(DBConfig.PASSWORD)
      && config.user == null && config.password != null) {
      collector.addFailure("Username and password should be provided together.", "Please provide both " +
        "user name and password, if database supports it or remove password.")
        .withConfigProperty(ConnectionConfig.USER).withConfigProperty(ConnectionConfig.PASSWORD);
    }

    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass(config.jdbcPluginType,
                                                                                config.jdbcPluginName,
                                                                                jdbcPluginId,
                                                                                PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      collector.addFailure(
        String.format("Unable to load JDBC Driver class for plugin name '%s'.", config.jdbcPluginName),
        String.format("Ensure that plugin '%s' of type '%s' containing the driver has been " +
                        "deployed.", config.jdbcPluginName, config.jdbcPluginType))
        .withConfigProperty(ConnectionConfig.JDBC_PLUGIN_NAME)
        .withPluginNotFound(jdbcPluginId, config.jdbcPluginName, config.jdbcPluginType);
    }

    return jdbcDriverClass;
  }

  public boolean tableExists(Class<? extends Driver> jdbcDriverClass, String tableName) {
    try {
      ensureJDBCDriverIsAvailable(jdbcDriverClass);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register JDBC driver {} while checking for the existence of the database table {}.",
                jdbcDriverClass, tableName, e);
      throw Throwables.propagate(e);
    }

    try (Connection connection = DriverManager.getConnection(config.connectionString,
                                                             config.getConnectionArguments())) {
      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet rs = metadata.getTables(null, null, tableName, null)) {
        return rs.next();
      }
    } catch (SQLException e) {
      LOG.error("Exception while trying to check the existence of database table {} for connection {}.",
                tableName, config.connectionString, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Ensures that the JDBC Driver specified in configuration is available and can be loaded. Also registers it with
   * {@link DriverManager} if it is not already registered.
   */
  public void ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass)
    throws IllegalAccessException, InstantiationException, SQLException {
    driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(jdbcDriverClass, config.connectionString,
                                                        config.jdbcPluginType, config.jdbcPluginName);
  }

  @Override
  public void destroy() {
    if (driverCleanup != null) {
      driverCleanup.destroy();
    }
  }
}
