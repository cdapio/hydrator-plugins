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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.annotation.Nullable;

/**
 * SSH into a remote machine and execute a script on that machine.
 * A user must specify username and keypair authentication credentials.
 * Options include port and machine URL.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SSH")
@Description("Action to run a script on remote machine")
public class SSHAction extends Action {
  private final SSHActionConfig config;

  private static final Logger LOG = LoggerFactory.getLogger(SSHAction.class);

  public SSHAction(SSHActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(final ActionContext context) throws Exception {
    // Now that macros have been substituted, try validation again
    config.validate();

    Connection connection = new Connection(config.host, config.port);
    try {
      connection.connect();

      if (!connection.authenticateWithPublicKey(config.user, config.privateKey.toCharArray(), config.passphrase)) {
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

      if (out.length() > 0) {
        LOG.debug("Stdout: {}", out);
      }
      if (err.length() > 0) {
        LOG.error("Stderr: {}", err);
      }
      Integer exitCode = session.getExitStatus();
      if (exitCode != null && exitCode != 0) {
        throw new IOException(String.format("Error: command %s running on hostname %s finished with exit code: %d",
                                            config.command, config.host, exitCode));
      }

      // Removes the carriage return at the end of the line
      out = out.endsWith("\n") ? out.substring(0, out.length() - 1) : out;
      context.getArguments().set(config.outputKey, out);
    } finally {
      connection.close();
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (!config.containsMacro("port")) {
      config.validate();
    }
  }

  /**
   * Config class that contains all the properties needed to SSH into the remote machine and run the script.
   */
  public static class SSHActionConfig extends PluginConfig {
    @Description("Command to be executed on the remote host, including file path of script and any arguments")
    @Macro
    private String command;

    @Description("Host name of the remote machine where the command is to be executed")
    @Macro
    private String host;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    @Macro
    private Integer port;

    @Description("User name used to connect to host")
    @Macro
    private String user;

    @Description("The private key to be used to perform the secure shell action. Can be a macro that " +
      "will pull the private key from the secure key management store in CDAP, such as ${secure(myPrivateKey)}.")
    @Macro
    private String privateKey;

    @Nullable
    @Description("Passphrase (if one) used to decrypt the provided private key associated with \"privateKey\"")
    @Macro
    private String passphrase;

    @Nullable
    @Description("The key used to store the output of the command run by the action. Plugins that run at later " +
      "stages in the pipeline can retrieve the command's output using this key through macro substitution: " +
      "${sshOutput} where \"sshOutput\" is the key specified. Defaults to \"sshOutput\".")
    @Macro
    private String outputKey;

    public SSHActionConfig() {
      this.port = 22;
      this.outputKey = "sshOutput";
    }

    @VisibleForTesting
    public SSHActionConfig(String command, String host, String user, String privateKeyFileLookupKey, Integer port,
                           String passphrase, String outputKey) {
      this.command = command;
      this.host = host;
      this.port = (port == null) ? 22 : port;
      this.user = user;
      this.privateKey = privateKeyFileLookupKey;
      this.passphrase = passphrase;
      this.outputKey = outputKey;
    }

    public void validate() {
      // Check that port is not negative
      if (!containsMacro("port") && port < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }
    }
  }
}
