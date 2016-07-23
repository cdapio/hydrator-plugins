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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;

import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.annotation.Nullable;

/**
 * SSH into a remote machine and execute a script on that machine.
 * A user must specify username and keypair authentication credentials.
 * Optionals include port and machine URL
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SSHAction")
@Description("Action to run a script on remote machine")
public class SSHAction extends Action {
  private final SSHActionConfig config;

  private static final Logger LOG = LoggerFactory.getLogger(SSHAction.class);

  public SSHAction(SSHActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(final ActionContext context) throws Exception {
    Connection connection = new Connection(config.host);
    try {
      connection.connect();

      String password = Strings.isNullOrEmpty(config.password) ? null : config.password;
      if (!connection.authenticateWithPublicKey(config.user, new File(config.privateKeyFile), password)) {
        throw new IOException(String.format("SSH authentication error when connecting to %s@%s on port %d",
                                            config.user, config.host, config.port));
      }

      LOG.debug("Connection established with the host {}", config.host);
      Session session = connection.openSession();
      session.execCommand(config.command);

      InputStream stdout = new StreamGobbler(session.getStdout());
      BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout, Charsets.UTF_8));
      InputStream stderr = new StreamGobbler(session.getStderr());
      BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr, Charsets.UTF_8));

      String out = CharStreams.toString(outBuffer);
      String err = CharStreams.toString(errBuffer);

      LOG.debug("Output: {}", out);
      LOG.error("Errors: {}", err);
      Integer exitCode = session.getExitStatus();
      if (exitCode != null && exitCode != 0) {
        throw new IOException(String.format("Error: command %s running on hostname %s finished with exit code: %d",
                                            config.command, config.host, exitCode));
      }

      if (config.output != null) {
        //removes the carriage return at the end of the line
        out = out.endsWith("\n") ? out.substring(0, out.length() - 1) : out;

        context.getArguments().set(config.output, out);
      }
    } finally {
      connection.close();
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    this.config.validate();
  }

  /**
   * Config class that contains all the properties needed to SSH into the remote machine and run the script.
   */
  public static class SSHActionConfig extends PluginConfig {

    @Description("Host name of the remote machine where the command needs to be executed.")
    private String host;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    private Integer port;

    @Description("User name used to connect to host")
    private String user;

    @Description("File path to Private key")
    private String privateKeyFile;

    @Nullable
    @Description("Password associated with private key")
    private String password;

    @Description("Command to be executed on the remote host. Should include filepath of script and any arguments")
    private String command;

    @Nullable
    @Description("Script output variable used as the key to store output from the script " +
      "to pass through Action Context")
    private String output;

    public SSHActionConfig() {
      port = 22;
    }

    @VisibleForTesting
    public SSHActionConfig(String host, String user, String privateKeyFile, int port, String password,
                           String command, String output) {
      this.host = host;
      this.port = port;
      this.user = user;
      this.privateKeyFile = privateKeyFile;
      this.password = password;
      this.command = command;
      this.output = output;
    }

    public void setCMD(String command) {
      this.command = command;
    }

    public void validate() {
      //check that port is not negative
      if (port < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }
    }
  }
}
