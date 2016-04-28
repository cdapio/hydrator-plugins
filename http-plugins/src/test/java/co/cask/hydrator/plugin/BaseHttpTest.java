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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestConfiguration;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.plugin.batch.HTTPCallbackAction;
import co.cask.hydrator.plugin.mock.MockFeedHandler;
import co.cask.hydrator.plugin.realtime.HTTPPollerRealtimeSource;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.HttpMethod;

/**
 * Base test for http plugin tests.
 */
public class BaseHttpTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("etlbatch", "3.2.0");
  protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("etlbatch", "3.2.0");
  protected static final ArtifactId REALTIME_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("etlrealtime", "3.2.0");
  protected static final ArtifactSummary REALTIME_ARTIFACT = new ArtifactSummary("etlrealtime", "3.2.0");

  private static int startCount;
  private static NettyHttpService httpService;
  protected static String baseURL;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(BATCH_ARTIFACT_ID, ETLBatchApplication.class);
    setupRealtimeArtifacts(REALTIME_ARTIFACT_ID, ETLRealtimeApplication.class);

    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(Id.Namespace.DEFAULT, BATCH_ARTIFACT_ID.getArtifact(),
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true,
                                  new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true));
    parents.add(new ArtifactRange(Id.Namespace.DEFAULT, REALTIME_ARTIFACT_ID.getArtifact(),
                                  new ArtifactVersion(REALTIME_ARTIFACT.getVersion()), true,
                                  new ArtifactVersion(REALTIME_ARTIFACT.getVersion()), true));
    addPluginArtifact(NamespaceId.DEFAULT.artifact("http-plugins", "1.0.0"), parents,
                      HTTPPollerRealtimeSource.class,
                      HTTPCallbackAction.class);


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

  @After
  public void cleanupTest() throws IOException {
    resetFeeds();
  }

  private int resetFeeds() throws IOException {
    URL url = new URL(baseURL + "/feeds");
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.DELETE);
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }

  protected int writeFeed(String feedId, String content) throws IOException {
    URL url = new URL(String.format("%s/feeds/%s", baseURL, feedId));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.getOutputStream().write(content.getBytes(Charsets.UTF_8));
    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();
    return responseCode;
  }

  protected String getFeedContent(String feedId) throws IOException {
    URL url = new URL(baseURL + "/feeds/" + feedId);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    try (Reader responseReader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8)) {
      return CharStreams.toString(responseReader);
    } finally {
      urlConn.disconnect();
    }
  }
}
