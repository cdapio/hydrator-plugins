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
import co.cask.cdap.test.TestBase;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.hydrator.common.test.MockEmitter;
import co.cask.hydrator.common.test.MockRealtimeContext;
import co.cask.hydrator.plugin.realtime.config.UrlFetchRealtimeSourceConfig;
import com.google.common.collect.ImmutableList;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * <p>
 *  Unit test for {@link UrlFetchRealtimeSource} ETL realtime source class.
 * </p>
 */
public class UrlFetchTest extends TestBase {
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

    UrlFetchRealtimeSourceConfig config = new UrlFetchRealtimeSourceConfig(
      String.format("http://%s:%s/ping",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      1L
    );
  }

  @After
  public void stopHttpService() {
    service.stopAndWait();
  }

  @Test
  public void testUrlFetch() throws Exception {
    UrlFetchRealtimeSourceConfig config = new UrlFetchRealtimeSourceConfig(
      String.format("http://%s:%s/ping",
                    service.getBindAddress().getHostName(),
                    service.getBindAddress().getPort()),
      1L
    );
    UrlFetchRealtimeSource source = new UrlFetchRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    SourceState state = new SourceState();

    source.poll(emitter, state);
    Assert.assertEquals(1, emitter.getEmitted().size());
    StructuredRecord urlData = emitter.getEmitted().get(0);
    Assert.assertNotNull(urlData);
    Assert.assertNotNull(urlData.get("url"));
    Assert.assertNotNull(urlData.get("body"));
  }

  // Simple service for testing connection to URL
  public static class PingHandler extends AbstractHttpHandler {
    @GET
    @Path("/ping")
    public void testGet(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK");
    }
  }
}
