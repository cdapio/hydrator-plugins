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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Runs a query after a pipeline run.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("VerticaBulkLoadAction")
@Description("Vertica bulk load plugin")
public class VerticaBulkLoadAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(VerticaBulkLoadAction.class);
  private final VerticaConfig config;

  public VerticaBulkLoadAction(VerticaConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    DriverManager.registerDriver((Driver) Class.forName("com.vertica.jdbc.Driver").newInstance());

    try {
      try (Connection connection = DriverManager.getConnection(config.connectionString, config.user, config.password)) {
        connection.setAutoCommit(false);
        // run Copy statement
        VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection) connection, config.copyStatement);
        // Keep running count of the number of rejects
        int totalRejects = 0;

        // start() starts the stream process, and opens the COPY command.
        stream.start();

        FileSystem fs = FileSystem.get(new Configuration());

        List<String> fileList = new ArrayList<>();
        FileStatus[] fileStatus = fs.listStatus(new Path(config.path));
        for (FileStatus fileStat : fileStatus) {
          fileList.add(fileStat.getPath().toString());
        }

        for (String file : fileList) {
          LOG.info("File being loaded: {}", file);
          Path path = new Path(file);

          FSDataInputStream inputStream = fs.open(path);
          // Add stream to the VerticaCopyStream
          stream.addStream(inputStream);

          // call execute() to load the newly added stream. You could
          // add many streams and call execute once to load them all.
          // Which method you choose depends mainly on whether you want
          // the ability to check the number of rejections as the load
          // progresses so you can stop if the number of rejects gets too
          // high. Also, high numbers of InputStreams could create a
          // resource issue on your client system.
          stream.execute();

          // Show any rejects from this execution of the stream load
          // getRejects() returns a List containing the
          // row numbers of rejected rows.
          List<Long> rejects = stream.getRejects();

          // The size of the list gives you the number of rejected rows.
          int numRejects = rejects.size();
          totalRejects += numRejects;
          LOG.info("Number of rows rejected: {}", numRejects);

        }

        // Finish closes the COPY command. It returns the number of
        // rows inserted.
        long results = stream.finish();

        LOG.info("Finish returned {}", results);
        LOG.info("Number of rows accepted: {}", stream.getRowCount());
        LOG.info("Total number of rows rejected: {}", totalRejects);

        // Commit the loaded data
        connection.commit();
      }
    } catch (Exception e) {
      LOG.error("Error running query {}.", config.copyStatement, e);
    }
  }

  /**
   *
   */
  public class VerticaConfig extends PluginConfig {
    public static final String CONNECTION_STRING = "connectionString";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    @Name(CONNECTION_STRING)
    @Description("JDBC connection string including database name.")
    @Macro
    public String connectionString;

    @Name(USER)
    @Description("User to use to connect to the specified database. Required for databases that " +
      "need authentication. Optional for databases that do not require authentication.")
    @Nullable
    @Macro
    public String user;

    @Name(PASSWORD)
    @Description("Password to use to connect to the specified database. Required for databases that " +
      "need authentication. Optional for databases that do not require authentication.")
    @Nullable
    @Macro
    public String password;

    @Name("copyStatement")
    @Description("Copy statement for vertica to bulk load. This query must use the COPY statement to load data from " +
      "STDIN. Unlike copying from a file on the host, you do not need superuser privileges to copy a stream. All " +
      "your user account needs is INSERT privileges on the target table.")
    @Macro
    public String copyStatement;

    @Name("path")
    @Description("File dir from where the files needs to be loaded.")
    @Macro
    public String path;

    public VerticaConfig() {
      super();
    }

  }
}
