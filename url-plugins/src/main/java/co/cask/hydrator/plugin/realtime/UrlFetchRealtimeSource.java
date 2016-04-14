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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.hydrator.plugin.realtime.config.UrlFetchRealtimeSourceConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Real Time Source to fetch data from a url.
 */
@Plugin(type = RealtimeSource.PLUGIN_TYPE)
@Name("URLFetch")
@Description("Fetch data from an external URL at a regular interval.")
public class UrlFetchRealtimeSource extends RealtimeSource<StructuredRecord> {
  private static final int TIMEOUT = 60 * 1000;
  private static final String METHOD = "GET";
  private static final String POLL_TIME_STATE_KEY = "lastPollTime";
  private static final Schema SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("url", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("responseCode", Schema.of(Schema.Type.INT)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  private final UrlFetchRealtimeSourceConfig config;

  public UrlFetchRealtimeSource(UrlFetchRealtimeSourceConfig config) {
    this.config = config;
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) throws Exception {
    byte[] lastPollTimeBytes = currentState.getState(POLL_TIME_STATE_KEY);
    if (lastPollTimeBytes != null) {
      long lastPollTime = Bytes.toLong(lastPollTimeBytes);
      long currentPollTime = System.currentTimeMillis() / 1000;
      long diff = currentPollTime - lastPollTime;
      if (config.getIntervalInSeconds() - diff > 0) {
        TimeUnit.SECONDS.sleep(1L);
        return currentState;
      }
    }
    try {
      URL url = new URL(config.getUrl());
      String response = "";
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(METHOD);
      connection.setConnectTimeout(TIMEOUT);
      connection.setInstanceFollowRedirects(config.shouldFollowRedirects());
      if (config.hasCustomRequestHeaders()) {
        // Set additional request headers if needed
        for (Map.Entry<String, String> requestHeader : config.getRequestHeadersMap().entrySet()) {
          connection.setRequestProperty(requestHeader.getKey(), requestHeader.getValue());
        }
      }
      try {
        if (connection.getResponseCode() >= 400 && connection.getErrorStream() != null) {
          response = CharStreams.toString(new InputStreamReader(connection.getErrorStream(), config.getCharset()));
        } else if (connection.getInputStream() != null) {
          response = CharStreams.toString(new InputStreamReader(connection.getInputStream(), config.getCharset()));
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
      writer.emit(createStructuredRecord(response, flattenedHeaders, connection.getResponseCode()));
      return currentState;
    } finally {
      currentState.setState(POLL_TIME_STATE_KEY, Bytes.toBytes(System.currentTimeMillis() / 1000));
    }
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
