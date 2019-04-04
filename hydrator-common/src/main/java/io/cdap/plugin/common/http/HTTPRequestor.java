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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility code for performing a get request and formatting it as a StructuredRecord.
 */
public class HTTPRequestor {
  public static final Schema SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("url", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("responseCode", Schema.of(Schema.Type.INT)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  private final HTTPPollConfig config;

  public HTTPRequestor(HTTPPollConfig config) {
    this.config = config;
  }

  public StructuredRecord get() throws IOException {
    URL url = new URL(config.getUrl());
    String response = "";
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(config.getConnectTimeout());
    connection.setReadTimeout(config.getReadTimeout());
    connection.setInstanceFollowRedirects(config.shouldFollowRedirects());
    // Set additional request headers
    for (Map.Entry<String, String> requestHeader : config.getRequestHeadersMap().entrySet()) {
      connection.setRequestProperty(requestHeader.getKey(), requestHeader.getValue());
    }
    int responseCode = connection.getResponseCode();
    try {
      if (connection.getErrorStream() != null) {
        try (Reader reader = new InputStreamReader(connection.getErrorStream(), config.getCharset())) {
          response = CharStreams.toString(reader);
        }
      } else if (connection.getInputStream() != null) {
        try (Reader reader = new InputStreamReader(connection.getInputStream(), config.getCharset())) {
          response = CharStreams.toString(reader);
        }
      }
    } finally {
      connection.disconnect();
    }

    Map<String, List<String>> headers = connection.getHeaderFields();
    Map<String, String> flattenedHeaders = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (!Strings.isNullOrEmpty(entry.getKey())) {
        // If multiple values for the same header exist, concatenate them
        flattenedHeaders.put(entry.getKey(), Joiner.on(',').skipNulls().join(entry.getValue()));
      }
    }
    return createStructuredRecord(response, flattenedHeaders, responseCode);
  }

  private StructuredRecord createStructuredRecord(String response,
                                                  Map<String, String> headerFields,
                                                  int responseCode) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(SCHEMA);
    recordBuilder
      .set("ts", System.currentTimeMillis())
      .set("url", config.getUrl())
      .set("responseCode", responseCode)
      .set("headers", headerFields)
      .set("body", response);
    return recordBuilder.build();
  }
}
