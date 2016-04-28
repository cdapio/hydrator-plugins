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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.BaseHttpTest;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 */
public class HttpCallbackActionTestRun extends BaseHttpTest {

  @Test
  public void testHTTPCallbackAction() throws Exception {
    String body = "samuel jackson, dwayne johnson, christopher walken";
    ETLStage action = new ETLStage(
      "http",
      new ETLPlugin("HTTPCallback", PostAction.PLUGIN_TYPE,
                    ImmutableMap.of("url", baseURL + "/feeds/users",
                                    "method", "PUT",
                                    "body", body),
                    null));

    ETLStage source = new ETLStage("source", MockSource.getPlugin("httpCallbackInput"));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin("httpCallbackOutput"));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(action)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "actionTest");
    ApplicationManager appManager = TestBase.deployApplication(appId, appRequest);

    WorkflowManager manager = appManager.getWorkflowManager("ETLWorkflow");
    manager.start();
    manager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(body, getFeedContent("users"));
  }

}
