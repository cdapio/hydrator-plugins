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
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.common.EchoCommandFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.shell.ProcessShellFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.EnumSet;

/**
 * Test for {@link OracleExportAction}
 */
public class OracleExportActionTest extends ETLBatchTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final String USER = "test";
  private static final String PASSPHRASE = "password";
  private static final String[] SHELL_ARGS = {"/bin/sh", "-i", "-l"};
  private static SshServer sshServer;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem fileSystem;

  @BeforeClass
  public static void buildMiniDFS() throws Exception {
    // Setup MiniDFSCluster
    File baseDir = TEMP_FOLDER.newFolder();
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();
    fileSystem = FileSystem.newInstance(conf);
    sshServer = setupSSHDaemon();
    sshServer.start();
  }

  @AfterClass
  public static void cleanupMiniDFS() throws Exception {
    // Shutdown MiniDFSCluster
    dfsCluster.shutdown();
    if (sshServer != null) {
      sshServer.stop();
    }
  }

  private static SshServer setupSSHDaemon() throws Exception {
    SshServer sshServer = SshServer.setUpDefaultServer();
    SimpleGeneratorHostKeyProvider simpleGeneratorHostKeyProvider = new SimpleGeneratorHostKeyProvider();
    simpleGeneratorHostKeyProvider.setAlgorithm(KeyUtils.RSA_ALGORITHM);
    sshServer.setKeyPairProvider(simpleGeneratorHostKeyProvider);
    sshServer.setPasswordAuthenticator(new PasswordAuthenticator() {
      @Override
      public boolean authenticate(String username, String password, ServerSession session) {
        return USER.equals(username) && PASSPHRASE.equals(password);
      }
    });
    sshServer.setShellFactory(new ProcessShellFactory(SHELL_ARGS));
    sshServer.setCommandFactory(EchoCommandFactory.INSTANCE);
    return sshServer;
  }

  @Test(expected = SSHAuthenticationException.class)
  public void testCheckWrongArgumentForServerPassword() throws Exception {
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, "Password", "wrongPassword", "", "",
                                                      "system", "cask", "home", "sid", "/tmp/junit.csv",
                                                      "select * from test;", "", "/tmp", "CSV");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    oracleExportAction.run(mockActionContext);
  }

  @Test
  public void testCheckWrongArgumentForFormat() throws Exception {
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, "Password", PASSPHRASE, "", "",
                                                      "system", "cask", "home", "sid", "/tmp/junit.csv",
                                                      "select * from test;", "", "/tmp", "format");

    try {
      MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
      new OracleExportAction(oracleExportActionConfig).configurePipeline(configurer);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format("Invalid format '%s'. Must be one of %s", "format",
                                        EnumSet.allOf(OracleExportAction.SeparatorFormat.class)), e.getMessage());
    }
  }

  @Test
  public void testOracleExportAction() throws Exception {
    Path outputDir = dfsCluster.getFileSystem().getHomeDirectory();
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, "Password",  PASSPHRASE, "", "",
                                                      "system", "cask", "home", "sid",
                                                      outputDir.toUri().toString() + "/tmp/OracleExportJunitTemp.csv",
                                                      "select * from test;", "", "/tmp", "CSV");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    oracleExportAction.run(mockActionContext);
    Assert.assertTrue(fileSystem.exists(new Path(outputDir.toUri().toString() + "/tmp/OracleExportJunitTemp.csv")));
  }
}
