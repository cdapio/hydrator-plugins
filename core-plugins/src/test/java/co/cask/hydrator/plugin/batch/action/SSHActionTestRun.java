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

import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
  * Test for {@link SSHAction}
  */
public class SSHActionTestRun extends ETLBatchTestBase {

  private static final String host = "localhost";
  private static final int port = 22;
  private static final String user = "Christopher";
  private static final String privateKeyFile = "/Users/Christopher/.ssh/id_rsa";
  private static final String privateKeyPassphrase = "thegreenfrogatcask";
  private static final String cmd = "./sshActionTestScript.sh localhost";

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
        .addConnection(action.getName(), sink.getName())
        .addConnection(source.getName(), sink.getName())
        .build();

      AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sshActionTest");
      ApplicationManager appManager = deployApplication(appId, appRequest);

      WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      manager.start(ImmutableMap.of("logical.start.time", "0"));
      manager.waitForFinish(3, TimeUnit.MINUTES);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
