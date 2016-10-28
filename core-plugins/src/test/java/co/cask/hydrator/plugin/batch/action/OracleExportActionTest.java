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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Test for {@link OracleExportAction}
 */
public class OracleExportActionTest extends ETLBatchTestBase {
  // Test keypair config
  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();
  private static final String PASSPHRASE = "password";
  private static final String[] SHELL_ARGS = {"/bin/sh", "-i", "-l"};
  private static final String USER = "test";
  // Test members
  private SshServer sshServer;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem fileSystem;

  @BeforeClass
  public static void buildMiniDFS() throws Exception {
    // Setup MiniDFSCluster
    File baseDir = folder.newFolder();
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    dfsCluster = builder.build();
    dfsCluster.waitActive();
    fileSystem = FileSystem.newInstance(conf);
  }

  @AfterClass
  public static void cleanupMiniDFS() throws Exception {
    // Shutdown MiniDFSCluster
    dfsCluster.shutdown();
    folder.delete();
  }

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
    sshServer.setPasswordAuthenticator(new PasswordAuthenticator() {
      @Override
      public boolean authenticate(String username, String password, ServerSession session) {
        return username.equals(USER) && password.equals(PASSPHRASE);
      }
    });
    sshServer.setShellFactory(new ProcessShellFactory(SHELL_ARGS));
    sshServer.setCommandFactory(EchoCommandFactory.INSTANCE);
    return sshServer;
  }

  @Test
  public void testCheckWrongArgumentForServerPassword() throws Exception {
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, "wrongPassword", "system",
                                                      "cask", "home", "sid", "/tmp/junit.csv",
                                                      "select * from test;", "csv");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    try {
      oracleExportAction.run(mockActionContext);
    } catch (IOException e) {
      Assert.assertEquals("SSH authentication error when connecting to test@" + sshServer.getHost() + " on port " +
                            sshServer.getPort(), e.getMessage());
    }
  }

  @Test
  public void testCheckWrongArgumentForQuery() throws Exception {
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, PASSPHRASE, "system",
                                                      "cask", "home", "sid", "/tmp/junit.csv",
                                                      "select * from test", "csv");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    try {
      oracleExportAction.run(mockActionContext);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Query should have ; at the end", e.getMessage());
    }
  }

  @Test
  public void testCheckWrongArgumentForFormat() throws Exception {
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, PASSPHRASE, "system",
                                                      "cask", "home", "sid", "/tmp/junit.csv",
                                                      "select * from test;", "format");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    try {
      oracleExportAction.run(mockActionContext);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Invalid format 'format'. Must be one of [csv, tsv, psv]", e.getMessage());
    }
  }

  @Test
  public void testOracleExportAction() throws Exception {
    Path outputDir = dfsCluster.getFileSystem().getHomeDirectory();
    OracleExportAction.OracleExportActionConfig oracleExportActionConfig =
      new OracleExportAction.OracleExportActionConfig(sshServer.getHost(),
                                                      sshServer.getPort(), USER, PASSPHRASE, "system",
                                                      "cask", "home", "sid",
                                                      outputDir.toUri().toString() + "/tmp/OracleExportJunitTemp.csv",
                                                      "select * from test;", "csv");
    OracleExportAction oracleExportAction = new OracleExportAction(oracleExportActionConfig);
    MockActionContext mockActionContext = new MockActionContext();
    oracleExportAction.run(mockActionContext);
    Assert.assertTrue(fileSystem.exists(new Path(outputDir.toUri().toString() + "/tmp/OracleExportJunitTemp.csv")));
  }
}
