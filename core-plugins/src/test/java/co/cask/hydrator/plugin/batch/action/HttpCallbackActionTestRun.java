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

import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import co.cask.hydrator.plugin.mock.MockFeedHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.HttpMethod;

/**
 */
public class HttpCallbackActionTestRun extends ETLBatchTestBase {
  private static NettyHttpService httpService;
  private static String baseURL;

  @BeforeClass
  public static void setup() throws Exception {
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new MockFeedHandler());
    httpService = NettyHttpService.builder()
      .addHttpHandlers(handlers)
      .build();
    httpService.startAndWait();

    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
    // tell service what its port is.
    URL setPortURL = new URL(baseURL + "/port");
    HttpURLConnection urlConn = (HttpURLConnection) setPortURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write(String.valueOf(port).getBytes(Charsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @AfterClass
  public static void teardown() {
    httpService.stopAndWait();
  }

  @Test
  public void testEmailAction() throws Exception {

    String body = "samuel jackson, dwayne johnson, christopher walken";
    ETLStage action = new ETLStage(
      "http",
      new ETLPlugin("HttpCallback", PostAction.PLUGIN_TYPE,
                    ImmutableMap.of("url", baseURL + "/feeds/users",
                                    "method", "PUT",
                                    "body", body),
                    null));

    ETLStage source = new ETLStage("source",
                                   new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of("name", "emailTestSource"), null));
    ETLStage sink = new ETLStage("sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                                       ImmutableMap.of("name", "emailTestSink"), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(action)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "actionTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager manager = appManager.getWorkflowManager("ETLWorkflow");
    manager.start();
    manager.waitForFinish(5, TimeUnit.MINUTES);

    URL url = new URL(baseURL + "/feeds/users");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(HttpMethod.GET);
    Assert.assertEquals(200, conn.getResponseCode());
    try (Reader responseReader = new InputStreamReader(conn.getInputStream(), Charsets.UTF_8)) {
      Assert.assertEquals(body, CharStreams.toString(responseReader));
    }
  }

}
