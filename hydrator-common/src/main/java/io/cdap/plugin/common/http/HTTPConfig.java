/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.common.http;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

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

  private static final String NAME_URL = "url";
  private static final String NAME_REQUEST_HEADERS = "requestHeaders";
  private static final String NAME_CONNECTION_TIMEOUT = "connectTimeout";

  @Description("The URL to fetch data from.")
  @Macro
  private String url;

  @Description("Request headers to set when performing the http request.")
  @Nullable
  @Macro
  private String requestHeaders;

  @Description("Whether to automatically follow redirects. Defaults to true.")
  @Nullable
  @Macro
  private Boolean followRedirects;

  @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute).")
  @Nullable
  @Macro
  private Integer connectTimeout;

  public HTTPConfig() {
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

  /**
   * Deprecated since 2.3.0.
   */
  @SuppressWarnings("ConstantConditions")
  @Deprecated
  public void validate() {
    validateURL();
    validateConnectionTimeout();
    validateRequestHeaders();
  }

  public void validate(FailureCollector collector) {
    try {
      validateURL();
    } catch (IllegalArgumentException e) {
      collector.addFailure(String.format("URL '%s' is malformed: %s", url, e.getMessage()), "Specify a valid url.")
        .withConfigProperty(NAME_URL).withStacktrace(e.getStackTrace());
    }

    try {
      validateConnectionTimeout();
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_CONNECTION_TIMEOUT);
    }

    convertHeadersToMap(requestHeaders, collector);
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

  private void validateURL() {
    if (!containsMacro(NAME_URL) && !Strings.isNullOrEmpty(url)) {
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(String.format("URL '%s' is malformed: %s", url, e.getMessage()), e);
      }
    }
  }

  private void validateConnectionTimeout() {
    if (!containsMacro(NAME_CONNECTION_TIMEOUT) && connectTimeout != null && connectTimeout < 0) {
      throw new IllegalArgumentException(String.format(
        "Invalid connectTimeout '%d'. Timeout must be 0 or a positive number.", connectTimeout));
    }
  }

  private void validateRequestHeaders() {
    if (!containsMacro(NAME_REQUEST_HEADERS)) {
      convertHeadersToMap(requestHeaders);
    }
  }

  private Map<String, String> convertHeadersToMap(String headersString, FailureCollector collector) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      for (String chunk : headersString.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER, 2);
        if (keyValue.length == 2) {
          headersMap.put(keyValue[0], keyValue[1]);
        } else {
          collector.addFailure(String.format("Unable to parse key-value pair '%s'.", chunk),
                               String.format("Ensure request headers are specified in <key>%s<value> format",
                                             KV_DELIMITER))
            .withConfigElement(NAME_REQUEST_HEADERS, chunk);
        }
      }
    }
    return headersMap;
  }
}
