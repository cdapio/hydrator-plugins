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
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
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
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import javax.annotation.Nullable;

/**
 * Action that exports data from Oracle.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("OracleExport")
@Description("A Hydrator Action plugin to efficiently export data from Oracle to HDFS or local file system. " +
  "The plugin uses Oracle's command line tools to export data. The data exported from this tool can then " +
  "be used in Hydrator pipelines.")
public class OracleExportAction extends Action {
  enum SeparatorFormat {
    CSV, TSV, PSV
  };

  private final OracleExportActionConfig config;

  public OracleExportAction(OracleExportActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    String oracleExportCommand = buildOracleExportCommand();
    Connection connection = new Connection(config.oracleServerHostname, config.oracleServerSSHPort);
    try {
      connection.connect();
      boolean isAuthenticated;
      if ("password".equalsIgnoreCase(config.authMechanism)) {
        isAuthenticated = connection.authenticateWithPassword(config.oracleServerUsername,
                                                              config.oracleServerPassword);
      } else {
        isAuthenticated = connection.authenticateWithPublicKey(config.oracleServerUsername,
                                                               config.privateKey.toCharArray(),
                                                               config.passphrase);
      }
      if (!isAuthenticated) {
        throw new SSHAuthenticationException(config.oracleServerUsername, config.oracleServerHostname,
                                             config.oracleServerSSHPort);
      }
      Session session = connection.openSession();
      session.execCommand(oracleExportCommand);
      try (InputStream stdout = new StreamGobbler(session.getStdout());
           BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout, Charsets.UTF_8));
           InputStream stderr = new StreamGobbler(session.getStderr());
           BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr, Charsets.UTF_8));) {
        Integer exitCode = session.getExitStatus();
        String err = CharStreams.toString(errBuffer);
        if (err.length() > 0) {
          throw new IOException("Error: " + err);
        }
        if (exitCode != null && exitCode != 0) {
          throw new IOException(String.format("Error running command %s on hostname %s; exit code: %d",
                                              oracleExportCommand, config.oracleServerHostname, exitCode));
        }

        // Removing lines other than the query results from the output
        StringBuffer out = new StringBuffer();
        String line;
        while ((line = outBuffer.readLine()) != null) {
          if (Strings.isNullOrEmpty(line.trim())) {
            break;
          }
          out.append(line + "\n");
        }

        //SQLPLUS command errors are not fetched from session.getStderr().
        //Errors and output received after executing the command in SQLPlus prompt are the one that
        //are printed on the SQL prompt
        if (out.toString().contains("ERROR at line")) {
          throw new IOException(String.format("Error executing sqlplus query %s on hostname %s; error message: %s",
                                              config.queryToExecute, config.oracleServerHostname, out));
        }
        Path file = new Path(config.outputPath);
        try (FileSystem fs = FileSystem.get(file.toUri(), new Configuration());
             FSDataOutputStream outStream = fs.create(file);
             BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8))) {
          br.write(out.toString());
        }
      }
    } finally {
      connection.close();
    }
  }

  private String buildOracleExportCommand() {
    String dbConnectionString = config.dbUsername + "/" + config.dbPassword + "@" + config.oracleSID;
    String colSeparator = getColSeparator(config.format);
    StringBuilder scriptContent = new StringBuilder();
    scriptContent.append("set colsep " + "\"" + colSeparator + "\"" + "\n");
    // 32767 is  the maximum limit a linesixe is allowed.
    scriptContent.append("set linesize 32767" + "\n");
    scriptContent.append("set newpage none" + "\n");
    scriptContent.append("set wrap off" + "\n");
    scriptContent.append("set trimspool on" + "\n");
    scriptContent.append("set trimout on" + "\n");
    // Set 0 to prevent printing of headings and avoid page breaks
    scriptContent.append("set pagesize 0" + "\n");
    scriptContent.append("set heading off" + "\n");
    scriptContent.append("spool on" + "\n");
    scriptContent.append(config.queryToExecute + "\n");
    scriptContent.append("spool off" + "\n");
    scriptContent.append("exit");
    //Set of commands to be executed in a session
    String setPath = "export ORACLE_HOME=" + config.oracleHome + "; export ORACLE_SID=" + config.oracleSID + ";";
    String tmpScriptCreationCommand = "echo '" + scriptContent.toString() + "' > " + config.tmpSQLScriptFile + ";";

    String sqlPlusSpoolExecutionCommand;
    if (Strings.isNullOrEmpty(config.commandToRun)) {
     sqlPlusSpoolExecutionCommand = config.oracleHome + "/bin" + "/sqlplus -s " + dbConnectionString +
      " @" + config.tmpSQLScriptFile;
    } else {
      sqlPlusSpoolExecutionCommand = config.commandToRun;
    }
    sqlPlusSpoolExecutionCommand += " |  sed 's/\\s*" + colSeparator + "\\s*/" + colSeparator + "/g';";

    String tmpSpoolScriptRemovalCommand = "rm " + config.tmpSQLScriptFile + "";
    return setPath + tmpScriptCreationCommand + sqlPlusSpoolExecutionCommand + tmpSpoolScriptRemovalCommand;
  }

  private String getColSeparator(String format) {
    switch (SeparatorFormat.valueOf(format.toUpperCase())) {
      case CSV:
        return ",";
      case TSV:
        return "  ";
      case PSV:
        return "|";
      default:
        throw new IllegalArgumentException(
          String.format("Invalid format '%s'. Must be one of %s", format, EnumSet.allOf(SeparatorFormat.class)));
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
    @Description("Port to use to SSH to the remote Oracle Host. Defaults to 22.")
    @Macro
    private Integer oracleServerSSHPort;

    @Description("Username to use to connect to the remote Oracle Host via SSH.")
    @Macro
    private String oracleServerUsername;

    @Description("Authentication mechanism to perform the secure shell action. Acceptable values " +
      "are Private Key, Password.")
    @Macro
    private String authMechanism;

    @Nullable
    @Description("The password to be used to perform the secure shell action. This will be ignored when " +
      "Private Key is used as the authentication mechanism.")
    @Macro
    private String oracleServerPassword;

    @Nullable
    @Description("The private key to be used to perform the secure shell action. This will be ignored " +
      "when Password is used as the authentication mechanism.")
    @Macro
    private String privateKey;

    @Nullable
    @Description("Passphrase used to decrypt the provided private key. This will be ignored " +
      "when Password is used as the authentication mechanism.")
    @Macro
    private String passphrase;

    @Description("Username to connect to the Oracle database.")
    @Macro
    private String dbUsername;

    @Description("Password to connect the Oracle database.")
    @Macro
    private String dbPassword;

    @Description("Absolute path of the ``ORACLE_HOME`` environment variable on the Oracle server host." +
      "This will be used to run the Oracle Spool utility.")
    @Macro
    private String oracleHome;

    @Description("Oracle System ID(SID). This is used to uniquely identify a particular database on the system.")
    @Macro
    private String oracleSID;

    @Description("Query to be executed for export. For example: select * from test where name='cask';")
    @Macro
    private String queryToExecute;

    @Nullable
    @Description("Absolute path of the temporary SQL script file which needs to be created. It will be removed " +
      "once the SQL command is executed. Default is /tmp/tmpHydrator.sql. If 'commandToRun' input is used, " +
      "then filename has to be as same as in 'commandToRun'.")
    @Macro
    private String tmpSQLScriptFile;

    @Nullable
    @Description("Oracle command to be executed on the Oracle host. When left blank, plugin will create " +
      "the command based on the input values provided in 'dbUsername', 'dbPassword', 'oracleHome', 'oracleSID', " +
      "'queryToExecute' and 'tmpSQLScriptDirectory'. " +
      "Format should be oracleHome/bin/sqlplus -s dbUsername/dbPassword@oracleSID @tmpSQLScriptFile")
    @Macro
    private String commandToRun;

    @Description("Path where output file will be exported.")
    @Macro
    private String outputPath;

    @Description("Format of the output file. Acceptable values are csv, tsv, psv.")
    @Macro
    private String format;

    public OracleExportActionConfig() {
      this.oracleServerSSHPort  = 22;
      this.tmpSQLScriptFile = "/tmp/tmpHydrator.sql";
    }

    public OracleExportActionConfig(String oracleServerHostname, @Nullable Integer oracleServerPort,
                                    String oracleServerUsername, String authMechanism, String oracleServerPassword,
                                    String privateKey, String passphrase, String dbUsername, String dbPassword,
                                    String oracleHome, String oracleSID,
                                    String outputPath, String queryToExecute, String commandToRun,
                                    String tmpSQLScriptDirectory, String format) {
      this.oracleServerHostname = oracleServerHostname;
      this.oracleServerSSHPort  = oracleServerPort;
      this.oracleServerUsername = oracleServerUsername;
      this.authMechanism = authMechanism;
      this.passphrase = passphrase;
      this.oracleServerPassword = oracleServerPassword;
      this.privateKey = privateKey;
      this.dbUsername = dbUsername;
      this.dbPassword = dbPassword;
      this.oracleHome = oracleHome;
      this.oracleSID = oracleSID;
      this.outputPath = outputPath;
      this.commandToRun = commandToRun;
      this.queryToExecute = queryToExecute;
      this.tmpSQLScriptFile = tmpSQLScriptDirectory;
      this.format = format;
    }

    public void validate() {
      if (!containsMacro("oracleServerPort") && oracleServerSSHPort  < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }

      if (!("password".equalsIgnoreCase(authMechanism) || "private key".equalsIgnoreCase(authMechanism))) {
        throw new IllegalArgumentException(
          String.format("Invalid authentication mechanism '%s'. Must be one of Private Key or Password",
                        authMechanism));
      }

      if ("password".equalsIgnoreCase(authMechanism) && Strings.isNullOrEmpty(oracleServerPassword)) {
        throw new IllegalArgumentException("Password cannot be empty");
      } else if ("private key".equalsIgnoreCase(authMechanism) && Strings.isNullOrEmpty(privateKey)) {
        throw new IllegalArgumentException("Private Key cannot be empty");
      }

      try {
        SeparatorFormat.valueOf(format.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid format '%s'. Must be one of %s", format, EnumSet.allOf(SeparatorFormat.class)));
      }

      String trimmedQuery = queryToExecute.trim();
      if (!trimmedQuery.endsWith(";")) {
        queryToExecute = trimmedQuery + ';';
      }
    }
  }
}
