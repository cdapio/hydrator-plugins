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

import co.cask.cdap.etl.mock.action.MockActionContext;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.EchoCommandFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.shell.ProcessShellFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.PublicKey;

/**
 * Test for {@link SSHAction}
 */
public class SSHActionTest extends ETLBatchTestBase {
  // Test keypair config
  private static final String PRIVATE_KEY =
    "-----BEGIN RSA PRIVATE KEY-----\n" +
    "Proc-Type: 4,ENCRYPTED\n" +
    "DEK-Info: AES-128-CBC,9CEAA64BCD8E8EF8A953C0282D39FE1B\n" +
    "\n" +
    "9wFTqXKQkvtHnzthva5k+vv73DMjeRXckRIiSlJeY61kDbKLCGqQWc3DHqeQZikz\n" +
    "vAAQFVUqMMX0MHt+mIJZa+7F1kYg6wo9jKr2BIKahwcdCJo5vDx/xBnz33VsGkWf\n" +
    "gF2lo5X2qQIDrjer6SJLciop9jb91KIkbwE1uR+6/84EWigQQxn1a7rZEQxyZQK+\n" +
    "wkeYfvWi5Ngav01v4Ui7KJA69+i8634WZrPvpNt1trIAPyW52EtnHA1MrOnKiWmh\n" +
    "qCmniJ9LoVThnNJekQMCTXi01DZMuWxx6FcZaaJVu7jlxkaWea6l0Nt+gSnkdyxZ\n" +
    "nb/DB/IC81gvQuUOxQ0gYlaZPkdCygRpFoWl2ssOuP1vBXqSdOeU61TFM45i4mJ9\n" +
    "/YBB2nsDy6WWstSCLCNQatysVIQwX58Rt19sqHurOS2wOgB4BFhFux/Umd865jmO\n" +
    "n74LeKM4V93mLpHjet2NF7MRnphOAf0en6cb1rAUFGSCVbVO9ogwxdvfHJcsHlTg\n" +
    "cWeSMvTl+YvnTua93qth1l2weJA783BJrXBOd1no5UADDPP6DOxmC90VGHnIrI7/\n" +
    "bmRxOxJeJbYHLKiTZ6AGcqhXN/G98uKVD20/hvBawf8=\n" +
    "-----END RSA PRIVATE KEY-----";
  private static final String PASSPHRASE = "password";
  private static final String EXPECTED_PUBLIC_KEY = "MHwwDQYJKoZIhvcNAQEBBQADawAwaAJhAJhY3ARbmyKD9432C9l/lTwSwcj98KjE" +
    "ifmUKst4WJOb3C0mpC0CTAqhZVgX9KlFwaRQYVzNRWZuxr/JJ7HcNU5Y2bJxbd7/lKFXE2L9gtwEzcA2bj5Oxw9cuKvSq66ZNwIDAQAB";

  // Daemon config
  private static final String[] SHELL_ARGS = {"/bin/sh", "-i", "-l"};
  private static final String EXPECTED_OUTPUT = "command successful";

  // SSHAction config
  private static final String COMMAND = "echo command successful";
  private static final String USER = "test";
  private static final String OUTPUT_KEY = "output";

  // Test members
  private SshServer sshServer;

  @Before
  public void setup() throws Exception {
    sshServer = setupSSHDaemon();
    sshServer.start();
  }

  @After
  public void cleanup() throws Exception {
    if (sshServer != null) {
      sshServer.stop();
    }
  }

  private SshServer setupSSHDaemon() throws Exception {
    SshServer sshServer = SshServer.setUpDefaultServer();
    SimpleGeneratorHostKeyProvider simpleGeneratorHostKeyProvider = new SimpleGeneratorHostKeyProvider();
    simpleGeneratorHostKeyProvider.setAlgorithm(KeyUtils.RSA_ALGORITHM);
    sshServer.setKeyPairProvider(simpleGeneratorHostKeyProvider);
    sshServer.setPublickeyAuthenticator(new PublickeyAuthenticator() {
      @Override
      public boolean authenticate(String username, PublicKey key, ServerSession session) {
        return username.equals(USER) && Base64.encodeBase64String(key.getEncoded()).equals(EXPECTED_PUBLIC_KEY);
      }
    });
    sshServer.setShellFactory(new ProcessShellFactory(SHELL_ARGS));
    sshServer.setCommandFactory(EchoCommandFactory.INSTANCE);
    return sshServer;
  }

  @Test
  public void testSSHAction() throws Exception {
    SSHAction.SSHActionConfig sshActionConfig = new SSHAction.SSHActionConfig(COMMAND, sshServer.getHost(), USER,
                                                                              PRIVATE_KEY, sshServer.getPort(),
                                                                              PASSPHRASE, OUTPUT_KEY);
    SSHAction sshAction = new SSHAction(sshActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    sshAction.run(mockActionContext);
    Assert.assertEquals(mockActionContext.getArguments().get(OUTPUT_KEY), EXPECTED_OUTPUT);
  }
}
