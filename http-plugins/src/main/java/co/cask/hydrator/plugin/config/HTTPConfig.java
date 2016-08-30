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

package co.cask.hydrator.plugin.config;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.base.Strings;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base config class for HTTP plugins.
 */
@SuppressWarnings("ConstantConditions")
public class HTTPConfig extends PluginConfig {
  // Used for parsing requestHeadersString into map<string, string>
  // Should be the same as the widgets json config
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";

  @Description("The URL to fetch data from.")
  private String url;

  @Description("Request headers to set when performing the http request.")
  @Nullable
  private String requestHeaders;

  @Description("Whether to automatically follow redirects. Defaults to true.")
  @Nullable
  private Boolean followRedirects;

  @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute).")
  @Nullable
  private Integer connectTimeout;

  public HTTPConfig() {
    this(null);
  }

  public HTTPConfig(String url) {
    this(null, null);
  }

  public HTTPConfig(String url, String requestHeaders) {
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.followRedirects = true;
    this.connectTimeout = 60 * 1000;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeaders);
  }

  public boolean shouldFollowRedirects() {
    return followRedirects;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  @SuppressWarnings("ConstantConditions")
  public void validate() {
    try {
      new URL(url);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(String.format("URL '%s' is malformed: %s", url, e.getMessage()), e);
    }
    if (connectTimeout < 0) {
      throw new IllegalArgumentException(String.format(
        "Invalid connectTimeout %d. Timeout must be 0 or a positive number.", connectTimeout));
    }
    convertHeadersToMap(requestHeaders);
  }

  private Map<String, String> convertHeadersToMap(String headersString) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      for (String chunk : headersString.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER, 2);
        if (keyValue.length == 2) {
          headersMap.put(keyValue[0], keyValue[1]);
        } else {
          throw new IllegalArgumentException(String.format("Unable to parse key-value pair '%s'.", chunk));
        }
      }
    }
    return headersMap;
  }
}
