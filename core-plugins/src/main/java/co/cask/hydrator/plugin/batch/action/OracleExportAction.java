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

package co.cask.hydrator.plugin.batch.action;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.EnumSet;
import javax.annotation.Nullable;

/**
 * Action that exports data from Oracle.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("OracleExport")
@Description("A Hydrator Action plugin to efficiently export data from Oracle to HDFS or local file system.\n" +
  "The plugin uses Oracle's command line tools to export data.\n" +
  "The data exported from this tool can then be used in Hydrator pipelines.")
public class OracleExportAction extends Action {
  private enum FORMATS {
    csv, tsv, psv
  };
  private final OracleExportActionConfig config;
  private String execute = null;

  public OracleExportAction(OracleExportActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    init();
    Connection connection = new Connection(config.oracleServerHostname, config.oracleServerPort);
    try {
      connection.connect();
      boolean isAuthenticated = connection.authenticateWithPassword(config.oracleServerUsername,
                                                                    config.oracleServerPassword);
      if (isAuthenticated == false) {
        throw new IOException(String.format("SSH authentication error when connecting to %s@%s on port %d",
                                            config.oracleServerUsername, config.oracleServerHostname,
                                            config.oracleServerPort));
      }
      Session session = connection.openSession();
      session.execCommand(execute);
      try (InputStream stdout = new StreamGobbler(session.getStdout());
           BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout, Charsets.UTF_8))) {
        Integer exitCode = session.getExitStatus();
        if (exitCode != null && exitCode != 0) {
          throw new IOException(String.format("Error running command %s on hostname %s; exit code: %d",
                                              execute, config.oracleServerHostname, exitCode));
        }
        String out = CharStreams.toString(outBuffer);
        //SQLPLUS command errors are not fetched from session.getStderr().
        //Errors and output received after executing the command in SQLPlus prompt are the one that
        //are printed on the SQL prompt
        if (out.contains("ERROR at line")) {
          throw new IOException(String.format("Error executing sqlplus query %s on hostname %s; error message: %s",
                                              config.queryToExecute, config.oracleServerHostname, out));
        }
        Path file = new Path(config.pathToWriteFinalOutput);
        FileSystem fs = FileSystem.get(file.toUri(), new Configuration());
        try (
          FSDataOutputStream outStream = fs.create(file);
          BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outStream, "UTF-8"))) {
          br.write(out.replaceAll("(?m)^[\\s&&[^\\n]]+|[\\s+&&[^\\n]]+$", "")); //Remove multiline trailing spaces
        }
      }
    } finally {
      connection.close();
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }

  private String validateQuery(String query) {
    if (!query.trim().endsWith(";")) {
      query += ';';
    }
    return query;
  }

  private void init() {
    StringBuilder scriptContent = null;
    String dbConnectionString = config.dbUsername + "/" + config.dbPassword + "@" + config.oracleSID;
    String colSeparator = getColSeparator(config.format);
    scriptContent = new StringBuilder();
    scriptContent.append("set colsep " + "\"" + colSeparator + "\"" + "\n");
    scriptContent.append("set linesize 10000" + "\n");
    scriptContent.append("set newpage none" + "\n");
    scriptContent.append("set wrap off" + "\n");
    scriptContent.append("set pagesize 0" + "\n");
    scriptContent.append("set heading off" + "\n");
    scriptContent.append("spool on" + "\n");
    scriptContent.append(validateQuery(config.queryToExecute) + "\n");
    scriptContent.append("spool off" + "\n");
    scriptContent.append("exit");
    //Set of commands to be executed in a session
    String setPath = "export ORACLE_HOME=" + config.oracleHome + "; export ORACLE_SID=" + config.oracleSID + ";";
    String createTempScriptToBeUsedForSpool = "echo '" + scriptContent.toString() + "' > /tmp/tmpHydrator.sql;";
    String executeSpoolInSqlplus = config.oracleHome + "/bin" + "/sqlplus -s " + dbConnectionString +
      " @/tmp/tmpHydrator.sql |  sed 's/\\s*" + colSeparator + "\\s*/" + colSeparator + "/g';";
    String removeTempScriptUsedForSpool = "rm /tmp/tmpHydrator.sql";
    execute = setPath + createTempScriptToBeUsedForSpool + executeSpoolInSqlplus + removeTempScriptUsedForSpool;
  }

  private String getColSeparator(String format) {
    switch (format.toLowerCase()) {
      case "csv":
        return ",";
      case "tsv":
        return "  ";
      case "psv":
        return "|";
      default:
        throw new IllegalArgumentException(
          String.format("Invalid format '%s'. Must be one of %s", format, EnumSet.allOf(FORMATS.class)));
    }
  }

  /**
   * Config class that contains all properties necessary to execute the SQLPLUs spool command.
   */
  public static class OracleExportActionConfig extends PluginConfig {

    @Description("Host name of the remote DB machine")
    @Macro
    private String oracleServerHostname;

    @Nullable
    @Description("Port of the remote DB machine. Defaults to 22")
    @Macro
    private Integer oracleServerPort;

    @Description("Username for remote DB host")
    @Macro
    private String oracleServerUsername;

    @Description("Password for remote DB host")
    @Macro
    private String oracleServerPassword;

    @Description("Username to connect to oracle DB")
    @Macro
    private String dbUsername;

    @Description("Password to connect to oracle DB")
    @Macro
    private String dbPassword;

    @Description("Path of the ORACLE_HOME")
    @Macro
    private String oracleHome;

    @Description("Oracle SID")
    @Macro
    private String oracleSID;

    @Description("Query to be executed for export")
    @Macro
    private String queryToExecute;

    @Description("Path where output file to be exported")
    @Macro
    private String pathToWriteFinalOutput;

    @Description("Format of the output file")
    @Macro
    private String format;

    public OracleExportActionConfig() {
      this.oracleServerPort = 22;
    }

    public OracleExportActionConfig(String oracleServerHostname, @Nullable Integer oracleServerPort,
                                    String oracleServerUsername, String oracleServerPassword,
                                    String dbUsername, String dbPassword,
                                    String oracleHome, String oracleSID,
                                    String pathToWriteFinalOutput, String queryToExecute, String format) {
      this.oracleServerHostname = oracleServerHostname;
      this.oracleServerPort = oracleServerPort;
      this.oracleServerUsername = oracleServerUsername;
      this.oracleServerPassword = oracleServerPassword;
      this.dbUsername = dbUsername;
      this.dbPassword = dbPassword;
      this.oracleHome = oracleHome;
      this.oracleSID = oracleSID;
      this.pathToWriteFinalOutput = pathToWriteFinalOutput;
      this.queryToExecute = queryToExecute;
      this.format = format;
    }

    public void validate() {
      if (!containsMacro("oracleServerPort") && oracleServerPort < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }
      if (!containsMacro(format) && !EnumUtils.isValidEnum(FORMATS.class, format)) {
        throw new IllegalArgumentException(
          String.format("Invalid format '%s'. Must be one of %s", format, EnumSet.allOf(FORMATS.class)));
      }
    }
  }
}
