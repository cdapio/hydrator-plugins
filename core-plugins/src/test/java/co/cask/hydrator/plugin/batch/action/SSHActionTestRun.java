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

import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.artifact.AppRequest;

import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.dumbster.smtp.SimpleSmtpServer;
import com.google.common.collect.ImmutableMap;
import com.jcraft.jsch.JSch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.swing.*;


/**
  * Test for {@link SSHAction}
  */
public class SSHActionTestRun extends ETLBatchTestBase {

  private static String host = "localhost";
  private static int port = 22;
  private static String user;
  private static String privateKeyFile;
  private static String privateKeyPassphrase = "pass";
  private static String filePath;
  private static String cmd = "mkdir -p dirFromSSHAction/subdir && touch dirFromSSHAction/createFile.txt " +
    "&& mv dirFromSSHAction/createFile.txt dirFromSSHAction/subdir";

  @Before
  public void beforeTest() throws Exception {
    try {
      user = System.getProperty("user.name");

      JSch jsch = new JSch();
      String passphrase = "pass";
      com.jcraft.jsch.KeyPair kpair = com.jcraft.jsch.KeyPair.genKeyPair(jsch, com.jcraft.jsch.KeyPair.RSA);
      kpair.setPassphrase(passphrase);
      kpair.writePrivateKey("/Users/" + user + "/.ssh/id_rsa");
      kpair.writePublicKey("/Users/" + user + "/.ssh/authorized_keys", "this is the public key");
      kpair.dispose();

      privateKeyFile = "/Users/" + user + "/.ssh/id_rsa";
      filePath = "/Users/" + user + "/dirFromSSHAction/subdir/createFile.txt";

    } catch (Exception e) {
      throw new Exception(String.format("Error when setting SSH authentication: %s", e.getMessage()));
    }

  }

  @Test
  public void testSSHAction() throws Exception {
    try {
      SSHAction sshAction = new SSHAction(
        new SSHAction.SSHActionConfig(host, user, privateKeyFile, privateKeyPassphrase, port, cmd));
      sshAction.run(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSSHActionPipeline() throws Exception {
    try {
      ImmutableMap.Builder builder = new ImmutableMap.Builder();
      builder.put("host", host);
      builder.put("port", Integer.toString(port));
      builder.put("user", user);
      builder.put("privateKeyFile", privateKeyFile);
      builder.put("privateKeyPassPhrase", privateKeyPassphrase);
      builder.put("cmd", cmd);
      ETLStage action = new ETLStage(
        "sshActionPOC",
        new ETLPlugin("SSHAction", Action.PLUGIN_TYPE, builder.build(), null));

      ETLStage source = new ETLStage("source",
                                     new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                                                   ImmutableMap.of("name", "sshActionTestSource"), null));
      ETLStage sink = new ETLStage("sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                                         ImmutableMap.of("name", "sshActionTestSink"), null));

      ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
        .addStage(source)
        .addStage(sink)
        .addStage(action)
        .addConnection(action.getName(), source.getName())
        .addConnection(source.getName(), sink.getName())
        .build();

      AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sshActionTest");
      ApplicationManager appManager = deployApplication(appId, appRequest);
      WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      manager.start(ImmutableMap.of("logical.start.time", "0"));
      manager.waitForFinish(3, TimeUnit.MINUTES);

      List<RunRecord> history = appManager.getHistory(new Id.Program(appId, ProgramType.WORKFLOW, SmartWorkflow.NAME),
                                                      ProgramRunStatus.FAILED);
      Assert.assertTrue(history.size() == 0); //make sure pipeline didn't fail

      File f = new File(filePath);
      Assert.assertTrue(f.exists());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFailedSSHActionPipeline() throws Exception {
    try {
      ImmutableMap.Builder builder = new ImmutableMap.Builder();
      builder.put("host", host);
      builder.put("port", Integer.toString(port));
      builder.put("user", user);
      builder.put("privateKeyFile", null);
      builder.put("privateKeyPassPhrase", privateKeyPassphrase);
      builder.put("cmd", cmd);
      ETLStage action = new ETLStage(
        "sshActionPOC",
        new ETLPlugin("SSHAction", Action.PLUGIN_TYPE, builder.build(), null));

      ETLStage source = new ETLStage("source",
                                     new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                                                   ImmutableMap.of("name", "sshActionTestSource"), null));
      ETLStage sink = new ETLStage("sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                                         ImmutableMap.of("name", "sshActionTestSink"), null));

      ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
        .addStage(source)
        .addStage(sink)
        .addStage(action)
        .addConnection(action.getName(), source.getName())
        .addConnection(source.getName(), sink.getName())
        .build();

      AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sshActionTest");
      ApplicationManager appManager = deployApplication(appId, appRequest);
      WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      manager.start(ImmutableMap.of("logical.start.time", "0"));
      manager.waitForFinish(3, TimeUnit.MINUTES);

      List<RunRecord> history = appManager.getHistory(new Id.Program(appId, ProgramType.WORKFLOW, SmartWorkflow.NAME),
                                                      ProgramRunStatus.FAILED);
      Assert.assertTrue(history.size() == 1); //make sure pipeline didn't fail

      Map<String, WorkflowNodeStateDetail> nodesInFailedProgram = manager.getWorkflowNodeStates(history.get(0).getPid());
      Assert.assertTrue(nodesInFailedProgram.size() == 1);

      //check that SSHAction node failed
      Assert.assertTrue(nodesInFailedProgram.values().iterator().next().getNodeStatus().equals(NodeStatus.FAILED));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
