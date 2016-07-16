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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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


/**
  * Test for {@link SSHAction}
  */
public class SSHActionTestRun extends ETLBatchTestBase {

  private static String host = "localhost";
  private static int port = 22;
  private static String user = "Christopher";
  private static String privateKeyFile = "/Users/Christopher/.ssh/id_rsa";
  private static String privateKeyPassphrase = "thegreenfrogatcask";
  private static String filePath = "/dirFromSSHAction/subdir/createFile.txt";
  private static String cmd = "mkdir -p dirFromSSHAction/subdir && touch dirFromSSHAction/createFile.txt " +
    "&& mv dirFromSSHAction/createFile.txt dirFromSSHAction/subdir";

  @Before
  public void beforeTest() {
//    try {
//      java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
//      host = localMachine.getHostName();
//      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", "SUN");
//      SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
//      keyGen.initialize(2048, random);
//      KeyPair pair = keyGen.generateKeyPair();
//      PrivateKey priv = pair.getPrivate();
//      PublicKey pub = pair.getPublic();
//
//      Signature rsa = Signature.getInstance("SHA1withRSA", "SUN");
//      rsa.initSign(priv);
//      FileInputStream fis = new FileInputStream("argv[0]");//what is going on here
//      BufferedInputStream bufin = new BufferedInputStream(fis);
//      byte[] buffer = new byte[1024];
//      int len;
//      while ((len = bufin.read(buffer)) >= 0) {
//        rsa.update(buffer, 0, len);
//      }
//      bufin.close();
//      byte[] realSig = rsa.sign();
//
//      //save signature bytes in private file
//      FileOutputStream sigfos = new FileOutputStream("id_rsa");
//      sigfos.write(realSig);
//      sigfos.close();
//      //save public key
//      byte[] key = pub.getEncoded();
//      FileOutputStream keyfos = new FileOutputStream("suepk");
//      keyfos.write(key);
//      keyfos.close();
//    } catch (Exception e) {
//      throw new Exception(String.format("Error when setting SSH authentication: %s", e.getMessage()));
//    }

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

//      AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
//      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sshActionTest");
////      ProgramId programId = new ProgramId(Id.Namespace.DEFAULT.getId(), appId.getId(), ProgramType.WORKFLOW,
////                                          SmartWorkflow.NAME);
//      ApplicationManager appManager = deployApplication(appId, appRequest);
//      WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
//      manager.start(ImmutableMap.of("logical.start.time", "0"));
//      manager.waitForFinish(3, TimeUnit.MINUTES);
//
//
//      List<RunRecord> history = appManager.getHistory(new Id.Program(appId, ProgramType.WORKFLOW, SmartWorkflow.NAME),
//                                                      ProgramRunStatus.FAILED);
//      Assert.assertTrue(history.size() == 0);
////      Map<String, WorkflowNodeStateDetail> nodesInFailedProgram =
////        manager.getWorkflowNodeStates(history.get(0).getPid());
//      //check that SSHAction node failed
////      Assert.assertTrue(nodesInFailedProgram.containsKey());
////      Assert.assertTrue(nodesInFailedProgram.get().getNodeStatus().equals(NodeStatus.FAILED));
////      File f = new File(filePath);
////      Assert.assertTrue(f.isFile());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
