/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.db.batch.source;

import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.JDBCDriverShim;
import co.cask.hydrator.plugin.db.batch.NoOpCommitConnection;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Class that extends {@link DBInputFormat} to load the database driver class correctly.
 */
public class ETLDBInputFormat extends DBInputFormat {
  public static final String AUTO_COMMIT_ENABLED = "co.cask.hydrator.db.autocommit.enabled";

  private static final Logger LOG = LoggerFactory.getLogger(ETLDBInputFormat.class);
  private Driver driver;
  private JDBCDriverShim driverShim;

  public static void setInput(Configuration conf,
                              Class<? extends DBWritable> inputClass,
                              String inputQuery,
                              String inputCountQuery,
                              boolean autoCommitEnabled) {
    DBConfiguration dbConf = new DBConfiguration(conf);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
    conf.setBoolean(AUTO_COMMIT_ENABLED, autoCommitEnabled);
  }

  @Override
  public Connection getConnection() {
    if (this.connection == null) {
      Configuration conf = getConf();
      try {
        String url = conf.get(DBConfiguration.URL_PROPERTY);
        try {
          // throws SQLException if no suitable driver is found
          DriverManager.getDriver(url);
        } catch (SQLException e) {
          if (driverShim == null) {
            if (driver == null) {
              ClassLoader classLoader = conf.getClassLoader();
              @SuppressWarnings("unchecked")
              Class<? extends Driver> driverClass =
                (Class<? extends Driver>) classLoader.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
              driver = driverClass.newInstance();

              // De-register the default driver that gets registered when driver class is loaded.
              DBUtils.deregisterAllDrivers(driverClass);
            }
            driverShim = new JDBCDriverShim(driver);
            DriverManager.registerDriver(driverShim);
            LOG.debug("Registered JDBC driver via shim {}. Actual Driver {}.", driverShim, driver);
          }
        }
        if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
          this.connection = DriverManager.getConnection(url);
        } else {
          this.connection = DriverManager.getConnection(url,
                                                        conf.get(DBConfiguration.USERNAME_PROPERTY),
                                                        conf.get(DBConfiguration.PASSWORD_PROPERTY));
        }

        boolean autoCommitEnabled = conf.getBoolean(AUTO_COMMIT_ENABLED, false);
        if (autoCommitEnabled) {
          // hack to work around jdbc drivers like the hive driver that throw exceptions on commit
          this.connection = new NoOpCommitConnection(this.connection);
        } else {
          this.connection.setAutoCommit(false);
        }
        this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return this.connection;
  }

  @Override
  protected RecordReader createDBRecordReader(DBInputSplit split, Configuration conf) throws IOException {
    final RecordReader dbRecordReader = super.createDBRecordReader(split, conf);
    return new RecordReader() {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        dbRecordReader.initialize(split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return dbRecordReader.nextKeyValue();
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException {
        return dbRecordReader.getCurrentKey();
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException {
        return dbRecordReader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return dbRecordReader.getProgress();
      }

      @Override
      public void close() throws IOException {
        try {
          dbRecordReader.close();
        } finally {
          try {
            DriverManager.deregisterDriver(driverShim);
          } catch (SQLException e) {
            LOG.error("Error de-registering driver shim. This does not matter for distributed mapreduce jobs, but " +
                        "may lead to memory leaks in local implementations where the jvm does not shut down " +
                        "after this mapreduce.", e);
          }
        }
      }
    };
  }

  @Override
  protected void closeConnection() {
    super.closeConnection();
    try {
      DriverManager.deregisterDriver(driverShim);
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

}
