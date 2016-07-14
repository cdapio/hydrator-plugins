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
 * License for the specific language governing permissions and litations under
 * the License.
 */

package co.cask.hydrator.plugin.batch.action;

import org.junit.Test;

/**
  * Test for {@link SSHAction}
  */
public class SSHActionTestRun {

  private static final String host = "localhost";
  private static final int port = 22;
  private static final String user = "Christopher";
  private static final String privateKeyFile = "/Users/Christopher/.ssh/id_rsa";
  private static final String privateKeyPassphrase = "thegreenfrogatcask";
  private static final String cmd = "mkdir -p dirFromSSHAction/subdir && touch dirFromSSHAction/createFile.txt " +
    "&& mv dirFromSSHAction/createFile.txt dirFromSSHAction/subdir";

  @Test
  public void testSSHAction() throws Exception {
    try {
      SSHAction sshAction = new SSHAction(
        new SSHAction.SSHActionConfig(host, port, user, privateKeyFile, privateKeyPassphrase, cmd));
      sshAction.run(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
