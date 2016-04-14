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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Makes an HTTP call at the end of a pipeline run.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("HttpCallback")
@Description("Makes an HTTP call at the end of a pipeline run.")
public class HttpCallbackAction extends PostAction {
  private static final Logger LOG = LoggerFactory.getLogger(HttpCallbackAction.class);
  private final HttpRequestConf conf;

  public HttpCallbackAction(HttpRequestConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    conf.validate();
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void run(BatchActionContext batchActionContext) throws Exception {
    int retries = 0;
    Exception exception = null;
    do {
      HttpURLConnection conn = null;
      Map<String, List<String>> headers = conf.parseRequestProperties();
      try {
        URL url = new URL(conf.url);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(conf.method);
        conn.setConnectTimeout(conf.connectTimeoutMillis);
        for (Map.Entry<String, List<String>> propertyEntry : headers.entrySet()) {
          String propertyKey = propertyEntry.getKey();
          for (String propertyValue : propertyEntry.getValue()) {
            conn.addRequestProperty(propertyKey, propertyValue);
          }
        }
        if (conf.body != null) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(conf.body.getBytes(Charsets.UTF_8));
          }
        }
        LOG.info("Request to {} resulted in response code {}.", conf.url, conn.getResponseCode());
        break;
      } catch (MalformedURLException | ProtocolException e) {
        // these should never happen because the url and request method are checked at configure time
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
        LOG.warn("Error making {} request to url {} with headers {}.", conf.method, conf.url, headers);
        exception = e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      retries++;
    } while (retries < conf.numRetries);

    if (exception != null) {
      throw exception;
    }
  }

  /**
   * Config for the http callback action.
   */
  public static final class HttpRequestConf extends PluginConfig {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Description("The URL to call.")
    private String url;

    @Nullable
    @Description("The http request method.")
    private String method;

    @Nullable
    @Description("The http request body.")
    private String body;

    @Nullable
    @Description("The connect timeout in milliseconds for the http request. Defaults to 0. " +
      "A timeout of 0 is interpreted as an infinite timeout.")
    private Integer connectTimeoutMillis;

    @Nullable
    @Description("The number of times the request should be retried if the request fails. Defaults to 0.")
    private Integer numRetries;

    @Nullable
    @Description("Properties to add to the request. Should be a JSONObject of string key values. " +
      "For example: {\"Accept-Language\", \"en-US,en;q=0.5\"}.")
    private String requestProperties;

    public HttpRequestConf() {
      this.url = null;
      this.method = null;
      this.connectTimeoutMillis = 0;
      this.numRetries = 0;
      this.requestProperties = null;
    }

    public Map<String, List<String>> parseRequestProperties() {
      // noinspection unchecked
      return requestProperties == null ?
        new HashMap<String, List<String>>() : (Map<String, List<String>>) GSON.fromJson(requestProperties, MAP_TYPE);
    }

    @SuppressWarnings("ConstantConditions")
    public void validate() {
      try {
        HttpMethod.valueOf(method);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("Invalid request method %s.", method));
      }
      if (connectTimeoutMillis < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid connectTimeoutMillis %d. Timeout cannot be a negative number.", connectTimeoutMillis));
      }
      if (numRetries < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid numRetries %d. Retries cannot be a negative number.", numRetries));
      }
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(String.format("URL '%s' is malformed: %s", url, e.getMessage()), e);
      }
    }
  }
}
