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

package co.cask.hydrator.plugin.realtime.config;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for URL Fetch plugin.
 */
public class UrlFetchRealtimeSourceConfig extends PluginConfig {
  private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;
  private static final int DEFAULT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_READ_TIMEOUT = 60 * 1000;
  // Used for parsing requestHeadersString into map<string, string>
  // Should be the same as the widgets json config
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";


  @Name("url")
  @Description("The URL to fetch data from.")
  private String url;

  @Name("interval")
  @Description("The amount of time to wait between each poll in seconds.")
  private long intervalInSeconds;

  @Name("headers")
  @Description("A set of additional request header values to set when fetching data.")
  @Nullable
  private String requestHeadersString;

  @Name("charset")
  @Description("The charset used to decode the response. Defaults to UTF-8.")
  @Nullable
  private String charsetString;

  @Name("followRedirects")
  @Description("Set true to follow redirects automatically.")
  @Nullable
  private Boolean followRedirects = true;

  @Name("connectTimeout")
  @Description("Sets the connection timeout in milliseconds. Default is 60000 (1 minute).")
  @Nullable
  private Integer connectTimeout = DEFAULT_CONNECT_TIMEOUT;

  @Name("readTimeout")
  @Description("Sets the read timeout in milliseconds. Default is 60000 (1 minute).")
  @Nullable
  private Integer readTimeout = DEFAULT_READ_TIMEOUT;


  public UrlFetchRealtimeSourceConfig(String url,
                                      long intervalInSeconds) {
    this.url = url;
    this.intervalInSeconds = intervalInSeconds;
  }

  public UrlFetchRealtimeSourceConfig(String url,
                                      long intervalInSeconds,
                                      String requestHeaders) {
    this.url = url;
    this.intervalInSeconds = intervalInSeconds;
    this.requestHeadersString = requestHeaders;
  }

  public UrlFetchRealtimeSourceConfig(String url,
                                      long intervalInSeconds,
                                      String requestHeaders,
                                      Charset charset,
                                      boolean followRedirects,
                                      int connectTimeout,
                                      int readTimeout) {
    this.url = url;
    this.intervalInSeconds = intervalInSeconds;
    this.requestHeadersString = requestHeaders;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.charsetString = charset.toString();
    this.followRedirects = followRedirects;
  }

  public String getUrl() {
    return url;
  }

  public long getIntervalInSeconds() {
    return intervalInSeconds;
  }

  public boolean hasCustomRequestHeaders() {
    return !Strings.isNullOrEmpty(requestHeadersString);
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeadersString);
  }

  public boolean shouldFollowRedirects() {
    return followRedirects;
  }

  public Charset getCharset() {
    return (Strings.isNullOrEmpty(charsetString)) ? DEFAULT_CHARSET : Charset.forName(charsetString);
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  private Map<String, String> convertHeadersToMap(String headersString) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      List<String> headerChunks = Arrays.asList(headersString.split(DELIMITER));
      for (String chunk : headerChunks) {
        List<String> keyValueList = Arrays.asList(chunk.split(KV_DELIMITER));
        headersMap.put(keyValueList.get(0),
                       Joiner.on(KV_DELIMITER).join(keyValueList.subList(1, keyValueList.size())));
      }
    }
    return headersMap;
  }

}
