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
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.ReferenceRealtimeSource;
import co.cask.hydrator.plugin.realtime.config.HTTPPollConfig;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Real Time Source to poll and fetch data from a url.
 */
@Plugin(type = RealtimeSource.PLUGIN_TYPE)
@Name("HTTPPoller")
@Description("Fetch data by performing an HTTP request at a regular interval.")
public class HTTPPollerRealtimeSource extends ReferenceRealtimeSource<StructuredRecord> {
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

  private final HTTPPollConfig config;

  public HTTPPollerRealtimeSource(HTTPPollConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    config.validate();
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) throws Exception {
    byte[] lastPollTimeBytes = currentState.getState(POLL_TIME_STATE_KEY);
    if (lastPollTimeBytes != null) {
      long lastPollTime = Bytes.toLong(lastPollTimeBytes);
      long currentPollTime = System.currentTimeMillis();
      long diffInSeconds = (currentPollTime - lastPollTime) / 1000;
      if (config.getInterval() - diffInSeconds > 0) {
        // This is a little bit of a workaround since clicking the stop button
        // in the UI will not interrupt a sleep. See: CDAP-5631
        TimeUnit.SECONDS.sleep(1L);
        return currentState;
      }
    }
    try {
      URL url = new URL(config.getUrl());
      String response = "";
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(METHOD);
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
      writer.emit(createStructuredRecord(response, flattenedHeaders, responseCode));
    } finally {
      currentState.setState(POLL_TIME_STATE_KEY, Bytes.toBytes(System.currentTimeMillis()));
    }
    return currentState;
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
