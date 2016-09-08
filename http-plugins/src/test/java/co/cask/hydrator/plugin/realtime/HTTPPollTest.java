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

package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.realtime.MockRealtimeContext;
import co.cask.cdap.test.TestBase;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.plugin.realtime.config.HTTPPollConfig;
import com.google.common.collect.ImmutableList;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * <p>
 *  Unit test for {@link HTTPPollerRealtimeSource} ETL realtime source class.
 * </p>
 */
public class HTTPPollTest extends TestBase {
  private NettyHttpService service;

  @Before
  public void setupHttpService() {
    // Setup HTTP service for testing and add Handlers
    service = NettyHttpService.builder()
      .setHost("localhost")
      .setPort(7777)
      .addHttpHandlers(ImmutableList.of(new PingHandler()))
      .build();
    service.startAndWait();
  }

  @After
  public void stopHttpService() {
    service.stopAndWait();
  }

  @Test
  public void testUrlFetch() throws Exception {
    HTTPPollConfig config = new HTTPPollConfig(
      "TestURLFetch",
      String.format("http://%s:%s/ping",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      1L);
    HTTPPollerRealtimeSource source = new HTTPPollerRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    SourceState state = new SourceState();

    source.poll(emitter, state);
    Assert.assertEquals(1, emitter.getEmitted().size());
    StructuredRecord urlData = emitter.getEmitted().get(0);
    Assert.assertNotNull(urlData);
    Assert.assertNotNull(urlData.get("url"));
    Assert.assertEquals("http://localhost:7777/ping", urlData.get("url"));
    Assert.assertNotNull(urlData.get("body"));
    Assert.assertEquals("OK", urlData.get("body"));
    Assert.assertNotNull(urlData.get("headers"));
    Assert.assertEquals("2", ((Map) urlData.get("headers")).get("Content-Length"));
    Assert.assertNotNull(urlData.get("responseCode"));
    Assert.assertEquals(200, urlData.get("responseCode"));
  }

  @Test
  public void testRequestHeaders() throws Exception {
    HTTPPollConfig config = new HTTPPollConfig(
      "TestRequestHeaders",
      String.format("http://%s:%s/useragent",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      1L,
      "User-Agent:T:est Us:er A:gent\nAccept:application/json");
    HTTPPollerRealtimeSource source = new HTTPPollerRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    SourceState state = new SourceState();

    source.poll(emitter, state);
    Assert.assertEquals(1, emitter.getEmitted().size());
    StructuredRecord urlData = emitter.getEmitted().get(0);
    Assert.assertNotNull(urlData);
    Assert.assertNotNull(urlData.get("url"));
    Assert.assertEquals("http://localhost:7777/useragent", urlData.get("url"));
    Assert.assertNotNull(urlData.get("body"));
    Assert.assertEquals("T:est Us:er A:gent", urlData.get("body"));
    Assert.assertNotNull(urlData.get("responseCode"));
    Assert.assertEquals(200, urlData.get("responseCode"));
  }

  @Test
  public void test404() throws Exception {
    HTTPPollConfig config = new HTTPPollConfig(
      "Test404",
      String.format("http://%s:%s/does-not-exist",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      1L);
    HTTPPollerRealtimeSource source = new HTTPPollerRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    SourceState state = new SourceState();

    source.poll(emitter, state);
    Assert.assertEquals(1, emitter.getEmitted().size());
    StructuredRecord urlData = emitter.getEmitted().get(0);
    Assert.assertNotNull(urlData);
    Assert.assertNotNull(urlData.get("url"));
    Assert.assertEquals("http://localhost:7777/does-not-exist", urlData.get("url"));
    Assert.assertNotNull(urlData.get("body"));
    Assert.assertEquals("Problem accessing: /does-not-exist. Reason: Not Found", urlData.get("body"));
    Assert.assertNotNull(urlData.get("responseCode"));
    Assert.assertEquals(404, urlData.get("responseCode"));
  }

  @Test
  public void testMultiplePolls() throws Exception {
    HTTPPollConfig config = new HTTPPollConfig(
      "TestMultiplePolls",
      String.format("http://%s:%s/ping",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      2L);
    HTTPPollerRealtimeSource source = new HTTPPollerRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    SourceState state = new SourceState();
    long start = System.currentTimeMillis();
    while (emitter.getEmitted().size() < 4) {
      source.poll(emitter, state);
    }
    long end = System.currentTimeMillis();
    Assert.assertTrue(end - start > 3 * 2 * 1000);
  }

  // Simple service for testing connection to URL
  public static class PingHandler extends AbstractHttpHandler {
    @GET
    @Path("/ping")
    public void testGet(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK");
    }

    @GET
    @Path("/useragent")
    public void testUserAgent(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, request.getHeader("User-Agent"));
    }
  }
}
