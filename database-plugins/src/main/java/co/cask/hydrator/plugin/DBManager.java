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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

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

  public void validateJDBCPluginPipeline(PipelineConfigurer pipelineConfigurer, String jdbcPluginId) {
    Preconditions.checkArgument(!(config.user == null && config.password != null),
                                "user is null. Please provide both user name and password if database requires " +
                                  "authentication. If not, please remove password and retry.");
    Class<? extends Driver> jdbcDriverClass = pipelineConfigurer.usePluginClass(config.jdbcPluginType,
                                                                                config.jdbcPluginName,
                                                                                jdbcPluginId,
                                                                                PluginProperties.builder().build());
    Preconditions.checkArgument(
      jdbcDriverClass != null, "Unable to load JDBC Driver class for plugin name '%s'. Please make sure that the " +
        "plugin '%s' of type '%s' containing the driver has been installed correctly.", config.jdbcPluginName,
      config.jdbcPluginName, config.jdbcPluginType);
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
