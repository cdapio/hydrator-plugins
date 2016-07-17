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
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.annotation.Nullable;

/**
 * SSH into a remote machine and execute a script on that machine.
<<<<<<< HEAD
 * A user must specify username and keypair authentication credentials.
=======
 * A user must specify username and key pair authentication credentials.
>>>>>>> 0b5a58b6ba2dfc9199791d3dadbf63038dbb5474
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

      LOG.info("DEBUG {}", config.privateKeyFile);
      char[] buff = new char[256];

      CharArrayWriter cw = new CharArrayWriter();

      FileReader fr = new FileReader(new File(config.privateKeyFile));

      while (true) {
        int len = fr.read(buff);
        if (len < 0) {
          break;
        }
        cw.write(buff, 0, len);
      }

      fr.close();

      LOG.info("File content is {}", new String(buff));

      String password = Strings.isNullOrEmpty(config.password) ? null : config.password;
      if (!connection.authenticateWithPublicKey(config.user, new File(config.privateKeyFile), password)) {
        throw new IOException(String.format("Unable to establish SSH connection for %s@%s on port %d",
                                            config.user, config.host, config.port));
      }

      LOG.info(String.format("Connection established with the host %s", config.host));

      Session session = connection.openSession();
      session.execCommand(config.cmd);

      InputStream stdout = new StreamGobbler(session.getStdout());
      BufferedReader outBuffer = new BufferedReader(new InputStreamReader(stdout));
      InputStream stderr = new StreamGobbler(session.getStderr());
      BufferedReader errBuffer = new BufferedReader(new InputStreamReader(stderr));

      StringBuilder outBuilder = new StringBuilder();
      StringBuilder errBuilder = new StringBuilder();

      String line = outBuffer.readLine();
      while (line != null) {
        outBuilder.append(line).append("\n");
        line = outBuffer.readLine();
      }

      line = errBuffer.readLine();
      while (line != null) {
        errBuilder.append(line).append("\n");
        line = errBuffer.readLine();
      }

      LOG.info("Command: {} and ExitStatus: {}", config.cmd, session.getExitStatus());
      LOG.info("Output: {}", outBuilder.toString());
      LOG.info("Errors: {}", errBuilder.toString());

      session.close();
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

    @Description("Host name of the remote machine where the command need to be executed.")
    private String host;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    private int port;

    @Description("User name used for conn")
    private String user;

    @Description("Private key file.")
    private String privateKeyFile;

    @Description("Password")
    private String password;

    @Description("Command to be executed on the remote host.")
    private String cmd;

    public SSHActionConfig(String host, String user, String privateKeyFile, int port, String cmd) {
      this.host = host;
      this.port = port;
      this.user = user;
      this.privateKeyFile = privateKeyFile;
      this.cmd = cmd;
    }

    public void validate() {
      //check that port is not negative
      if (port < 0) {
        throw new IllegalArgumentException("Port cannot be negative");
      }
    }
  }
}
