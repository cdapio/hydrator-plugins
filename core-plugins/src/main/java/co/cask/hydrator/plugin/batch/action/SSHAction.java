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
  public void run(ActionContext context) throws Exception {
    try {
      Connection connection = new Connection(config.host);
      connection.connect();

      if (!connection.authenticateWithPublicKey(config.user, new File(config.privateKeyFile),
                                                config.privateKeyPassPhrase)) {
        throw new IOException(String.format("Unable to establish SSH connection for %s@%s on port %d",
                                            config.user, config.host, config.port));
      }

      Session session = connection.openSession();

      try {
        session.execCommand(config.cmd);
      } catch (IOException e) {
        throw new IOException(String.format("Command failed with IOException: %s", e.getMessage()));
      }

      // Read stdout and stderr
      InputStream stdout = new StreamGobbler(session.getStdout());
      BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout));
      InputStream stderr = new StreamGobbler(session.getStderr());
      BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr));


      StringBuilder outBuilder = new StringBuilder();
      String line = outBuffer.readLine();
      while (line != null) {
        outBuilder.append(line + "\n");
        line = outBuffer.readLine();
      }

      StringBuilder errBuilder = new StringBuilder();
      line = errBuffer.readLine();
      while (line != null) {
        errBuilder.append(line + "\n");
        line = errBuffer.readLine();
      }

      LOG.info("Output:");
      LOG.info(outBuilder.toString());
      LOG.info("Errors:");
      LOG.info(errBuilder.toString());

      session.close();
    } catch (IOException e) {
      LOG.error("Error during SSHAction execution: ", e);
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

    @Nullable
    @Description("URL of host machine. Defaults to localhost")
    private String host;

    @Description("User login credentials")
    private String user;

    @Description("Path to Private Key File")
    private String privateKeyFile;

    @Description("Private Key Passphrase")
    private String privateKeyPassPhrase;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    private int port;

    @Nullable
    @Description("Script command")
    private String cmd; //command should include filepath and arguments

    public SSHActionConfig() {
      host = "localhost";
      port = 22;
    }

    public SSHActionConfig(String host, String user, String privateKeyFile, String privateKeyPassPhrase, int port,
                           String cmd) {
      this.host = host;
      this.user = user;
      this.privateKeyFile = privateKeyFile;
      this.privateKeyPassPhrase = privateKeyPassPhrase;
      this.port = port;
      this.cmd = cmd;
    }

    public void validate() {
      //check that only password or privateKey is set


      //check that port is not negative
      if (port < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }
    }
  }
}
