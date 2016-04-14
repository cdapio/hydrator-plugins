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

package co.cask.hydrator.plugin.mock;

import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Http handler used for unit tests.
 *
 * Has endpoints to set feed content, as well as endpoints to get feed content.
 *
 * PUT /port sets the port for the GET /feeds call, since the handler doesn't know what port it was bound to.
 * PUT /feeds/{feed-id} adds a feed. Feed content will be the request body.
 * GET /feeds/{feed-id} gets the content for a feed.
 * GET /feeds gets a list of all feeds.
 * DELETE /feeds deletes all feeds.
 */
public class MockFeedHandler implements HttpHandler {
  private Map<String, String> feeds;
  private Integer port;

  @Override
  public void init(HandlerContext handlerContext) {
    feeds = new HashMap<>();
  }

  @Override
  public void destroy(HandlerContext handlerContext) {
    // no-op
  }

  // the handler itself doesn't know the host and port that it will be bound to,
  // so adding a method to set it.
  @PUT
  @Path("port")
  public void setPort(HttpRequest request, HttpResponder responder) {
    port = Integer.parseInt(request.getContent().toString(Charsets.UTF_8));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  // the handler itself doesn't know the host and port that it will be bound to,
  // so adding a method to set it.
  @PUT
  @Path("feeds/{feed-id}")
  public void setBasePath(HttpRequest request, HttpResponder responder,
                          @PathParam("feed-id") String feedId) {
    String content = request.getContent().toString(Charsets.UTF_8);
    feeds.put(feedId, content);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  // returns an xml of all feed urls
  @GET
  @Path("feeds")
  public void getFeeds(HttpRequest request, HttpResponder responder) {
    if (port == null) {
      responder.sendStatus(HttpResponseStatus.CONFLICT);
      return;
    }
    StringBuilder response = new StringBuilder("<xml><urls>");
    for (Map.Entry<String, String> feedEntry : feeds.entrySet()) {
      response.append("<url>http://localhost:")
        .append(port)
        .append("/feeds/")
        .append(feedEntry.getKey())
        .append("</url>");
    }
    response.append("</urls></xml>");
    responder.sendString(HttpResponseStatus.OK, response.toString());
  }

  @DELETE
  @Path("feeds")
  public void deleteFeeds(HttpRequest request, HttpResponder responder) {
    feeds.clear();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("feeds/{feed-id}")
  public void getFeed(HttpRequest request, HttpResponder responder,
                      @PathParam("feed-id") String feedId) {
    if (!feeds.containsKey(feedId)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    responder.sendString(HttpResponseStatus.OK, feeds.get(feedId));
  }
}
