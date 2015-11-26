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

package co.cask.hydrator.plugin.sink.output;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * An {@link OutputFormat} which extends {@link TextOutputFormat} and overrides
 * {@link TextOutputFormat#getOutputCommitter(TaskAttemptContext)} to provide a custom {@link BulkOutputCommitter}
 * which delegates to {@link FileOutputCommitter} from the {@link TextOutputFormat} but in
 * {@link FileOutputCommitter#commitJob(JobContext)} also bulk load all data committed in the output files to the
 * given Vertica table.
 *
 * @param <K> key class
 * @param <V> value class
 */
public class BulkOutputFormat<K, V> extends TextOutputFormat<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(BulkOutputFormat.class);

  public static final String VERTICA_HOST_KEY = "verticaHost";
  public static final String VERTICA_USER_KEY = "verticsUser";
  public static final String VERTICA_PASSOWORD_KEY = "verticaPassword";
  public static final String VERTICA_TABLE_NAME = "verticaTable";
  public static final String VERTICA_TEXT_DELIMITER = "verticaDelimiter";
  public static final String VERTICA_DIRECT_MODE = "verticaDirect";
  private static final String BLANKSPACE = " ";


  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    final FileOutputCommitter delegateCommitter = (FileOutputCommitter) super.getOutputCommitter(context);
    return new BulkOutputCommitter(delegateCommitter);
  }

  /**
   * This class extends {@link OutputCommitter} and delegates all its calls to {@link FileOutputCommitter} given during
   * its construction but in the {@link OutputCommitter#commitJob(JobContext)} after delegate call it also bulk load
   * all the data to the given vertica table.
   */
  public class BulkOutputCommitter extends OutputCommitter {

    public static final String VERTICA_JDBC_DRIVER_CLASS = "com.vertica.jdbc.Driver";
    private Statement connStatement;

    final FileOutputCommitter delegateCommitter;

    /**
     * @param delegateCommitter A {@link FileOutputCommitter} to which all calls be delegated.
     */
    public BulkOutputCommitter(FileOutputCommitter delegateCommitter) {
      this.delegateCommitter = delegateCommitter;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      delegateCommitter.setupJob(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
      delegateCommitter.setupTask(taskContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      return delegateCommitter.needsTaskCommit(taskContext);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      delegateCommitter.commitTask(taskContext);
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
      delegateCommitter.abortTask(taskContext);
    }

    /**
     * Bulk load the data to Vertica table in addition to delegation call
     *
     * @param jobContext the {@link JobContext} of the job
     * @throws IOException
     */
    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      delegateCommitter.commitJob(jobContext);

      // bulk load to Vertica table
      // get all the committed files
      Path outputPath = getOutputPath(jobContext);
      Configuration conf = jobContext.getConfiguration();
      FileSystem fs = outputPath.getFileSystem(conf);
      RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(outputPath, false);

      // bulk load every file to Vertical table
      while (locatedFileStatusRemoteIterator.hasNext()) {
        LocatedFileStatus curFile = locatedFileStatusRemoteIterator.next();
        Path path = curFile.getPath();

        // only bulk load files containing data others like _SUCCESS should be skipped
        if (path.getName().contains("part")) {
          try {
            String copyStatement = getCopyStatement(conf.get(VERTICA_TABLE_NAME),
                                                    path.toUri().getPath(), conf.get(VERTICA_TEXT_DELIMITER),
                                                    Boolean.parseBoolean(conf.get(VERTICA_DIRECT_MODE)));
            LOG.info("Executing bulk load with statement {}", copyStatement);
            getConnStatement(conf.get(VERTICA_HOST_KEY), conf.get(VERTICA_USER_KEY),
                             conf.get(VERTICA_PASSOWORD_KEY)).execute(copyStatement);
          } catch (SQLException e) {
            LOG.info("Failed to execute to Vertica bulk load statement", e);
            Throwables.propagate(e);
          }
        }
      }
    }

    private String getCopyStatement(String tableName, String filePath, String delimiter, boolean directMode) {
      StringBuilder copyCommandBuilder = new StringBuilder("COPY");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append(tableName);
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("FROM");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("'").append(filePath).append("'");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("DELIMITER");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("'").append(delimiter).append("'");
      if (directMode) {
        copyCommandBuilder.append(BLANKSPACE);
        copyCommandBuilder.append("DIRECT");
      }
      return copyCommandBuilder.toString();
    }

    /**
     * Prepare the connection statement to Vertica
     *
     * @param dbHost jdbc path to the vertica database
     * @param username usernme of the db
     * @param password password for the user
     * @return {@link Statement} for the connection to Vertica database
     * @throws SQLException
     */
    private Statement getConnStatement(String dbHost, String username, String password) throws SQLException {
      try {
        Class.forName(VERTICA_JDBC_DRIVER_CLASS);
      } catch (ClassNotFoundException e) {
        LOG.error("Could not find the JDBC driver class {} for Vertica", VERTICA_JDBC_DRIVER_CLASS, e);
        Throwables.propagate(e);
      }
      if (connStatement == null) {
        Properties myProp = new Properties();
        myProp.put("user", username);
        myProp.put("password", password);
        Connection connection = DriverManager.getConnection(dbHost, myProp);
        connStatement = connection.createStatement();
      }
      return connStatement;
    }

    /**
     * Use the {@link FileOutputCommitter#getWorkPath()}
     */
    public Path getWorkPath() throws IOException {
      return delegateCommitter.getWorkPath();
    }
  }

  /**
   * Override this as the {@link TextOutputFormat#getDefaultWorkFile(TaskAttemptContext, String)} do explicit cast to
   * {@link FileOutputCommitter} and this will cause faliure as the {@link BulkOutputFormat} uses
   * {@link BulkOutputCommitter} an overridden {@link FileOutputCommitter}
   */
  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
    @SuppressWarnings("unchecked")
    BulkOutputCommitter committer = (BulkOutputCommitter) getOutputCommitter(context);
    return new Path(committer.getWorkPath(), getUniqueFile(context, getOutputName(context), extension));
  }
}
