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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import javax.annotation.Nullable;

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

  public static final String VERTICA_HOST_KEY = "verticaHost";
  public static final String VERTICA_USER_KEY = "verticsUser";
  public static final String VERTICA_PASSOWORD_KEY = "verticaPassword";
  public static final String VERTICA_TABLE_NAME = "verticaTable";
  public static final String VERTICA_TEXT_DELIMITER = "verticaDelimiter";
  public static final String VERTICA_DIRECT_MODE = "verticaDirect";
  public static final String HDFS_NAMENODE_ADDR = "webhdfsAddress";
  public static final String HDFS_USER = "hdfsUser";
  public static final String HDFS_NAMENODE_WEBHDFS_PORT = "webhdfsPort";
  private static final Logger LOG = LoggerFactory.getLogger(BulkOutputFormat.class);
  private static final String BLANKSPACE = " ";


  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    final FileOutputCommitter delegateCommitter = (FileOutputCommitter) super.getOutputCommitter(context);
    return new BulkOutputCommitter(delegateCommitter);
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

  /**
   * This class extends {@link OutputCommitter} and delegates all its calls to {@link FileOutputCommitter} given during
   * its construction but in the {@link OutputCommitter#commitJob(JobContext)} after delegate call it also bulk load
   * all the data to the given vertica table.
   */
  public class BulkOutputCommitter extends OutputCommitter {

    public static final String VERTICA_JDBC_DRIVER_CLASS = "com.vertica.jdbc.Driver";
    final FileOutputCommitter delegateCommitter;
    private Statement connStatement;
    private Boolean isLocalFS = null;

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
      detectFileSystem(fs);
      RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(outputPath, false);

      // bulk load every file to Vertical table
      while (locatedFileStatusRemoteIterator.hasNext()) {
        LocatedFileStatus curFile = locatedFileStatusRemoteIterator.next();
        Path path = curFile.getPath();

        // only bulk load files containing data others like _SUCCESS should be skipped
        if (path.getName().contains("part")) {
          String copyStatement = null;
          try {
            copyStatement = getCopyStatement(conf.get(VERTICA_TABLE_NAME),
                                             path, conf.get(HDFS_USER),
                                             conf.get(VERTICA_TEXT_DELIMITER),
                                             Boolean.parseBoolean(conf.get(VERTICA_DIRECT_MODE)), conf);
            LOG.info("Executing bulk load with statement {}", copyStatement);
            getConnStatement(conf.get(VERTICA_HOST_KEY), conf.get(VERTICA_USER_KEY),
                             conf.get(VERTICA_PASSOWORD_KEY)).execute(copyStatement);
          } catch (SQLException e) {
            LOG.info("Failed to execute to Vertica bulk load statement: {}", copyStatement, e);
            Throwables.propagate(e);
          }
        }
      }
    }

    private String getCopyStatement(String tableName, Path path, @Nullable String hdfsUser, String delimiter,
                                    boolean directMode, Configuration conf) throws IOException {
      if (isLocalFS) {
        return getLocalCopyStatement(tableName, path.toUri().getPath(), delimiter, directMode);
      } else {
        return getHDFSCopyStatement(tableName, getWebHDFSURL(path, conf), hdfsUser, delimiter, directMode);
      }
    }

    private void detectFileSystem(FileSystem fs) {
      if (isLocalFS == null) {
        if (fs instanceof DistributedFileSystem) {
          isLocalFS = false;
        } else if (fs instanceof LocalFileSystem) {
          isLocalFS = true;
        } else {
          throw new RuntimeException("Unknown filesystem " + fs.getUri());
        }
      }
    }

    private String getLocalCopyStatement(String tableName, String filePath, String delimiter,
                                         boolean directMode) {
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

    private String getHDFSCopyStatement(String tableName, String filePath, String hdfsUser, String delimiter,
                                        boolean directMode) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsUser), "A HDFS user must be provided while running in " +
        "Distributed mode.");
      StringBuilder copyCommandBuilder = new StringBuilder("COPY");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append(tableName);
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("SOURCE");
      copyCommandBuilder.append(BLANKSPACE);
      copyCommandBuilder.append("Hdfs(");
      copyCommandBuilder.append("url=").append("'").append(filePath).append("'").append(", ");
      copyCommandBuilder.append("username=").append("'").append(hdfsUser).append("'").append(")");
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

    private String getWebHDFSURL(Path path, Configuration conf) throws IOException {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(conf.get(HDFS_NAMENODE_ADDR)), "A HDFS Namenode must be " +
        "provided while running in Distributed mode.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(conf.get(HDFS_NAMENODE_WEBHDFS_PORT)), "A HDFS Namenode Port" +
        " must be provided while running in Distributed mode.");
      return String.format("http://%s:%s/webhdfs/v1%s", conf.get(HDFS_NAMENODE_ADDR),
                           conf.get(HDFS_NAMENODE_WEBHDFS_PORT), path.toUri().getPath());
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
        myProp.put("password", password == null ? "" : password);
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
}
