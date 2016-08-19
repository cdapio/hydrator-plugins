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

import co.cask.hydrator.plugin.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Class used by database action plugins to run database commands
 */
public class DBRun {
  private final QueryConfig config;
  private final Class<? extends Driver> driverClass;
  private static final Logger LOG = LoggerFactory.getLogger(DBRun.class);

  public DBRun(QueryConfig config, Class<? extends Driver> driverClass) {
    this.config = config;
    this.driverClass = driverClass;
  }

  public void run() {
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
}
