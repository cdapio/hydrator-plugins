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
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
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
  private static final Logger LOG = LoggerFactory.getLogger(UrlFetchRealtimeSource.class);

  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("url", Schema.of(Schema.Type.STRING)),
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
    URL url = new URL(config.getUrl());
    String response;
    URLConnection rawConnection = url.openConnection();
    try {
      HttpURLConnection connection = (HttpURLConnection) rawConnection;
      try {
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
          response = Bytes.toString(ByteStreams.toByteArray(connection.getInputStream()));
        } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
          response = Bytes.toString(ByteStreams.toByteArray(connection.getErrorStream()));
        } else {
          throw new Exception("Invalid response code returned: " + connection.getResponseCode());
        }
      } finally {
        connection.disconnect();
      }
    } catch (ClassCastException e) {
      // Used in test class for reading from file url
      if (e.getMessage().contains("FileURLConnection cannot be cast to java.net.HttpURLConnection")) {
        response = Bytes.toString(ByteStreams.toByteArray(rawConnection.getInputStream()));
      } else {
        throw e;
      }
    }
    Map<String, List<String>> headers = rawConnection.getHeaderFields();
    Map<String, String> flattenedHeaders = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (!Strings.isNullOrEmpty(entry.getKey())) {
        // If multiple values for the same header exist, concatenate them
        flattenedHeaders.put(entry.getKey(), Joiner.on(',').skipNulls().join(entry.getValue()));
      }
    }
    writeDefaultRecords(writer, response, flattenedHeaders);
    TimeUnit.SECONDS.sleep(config.getIntervalInSeconds());
    return currentState;
  }

  private void writeDefaultRecords(Emitter<StructuredRecord> writer,
                                   String response,
                                   Map<String, String> headerFields) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(DEFAULT_SCHEMA);
    recordBuilder
      .set("ts", System.currentTimeMillis())
      .set("url", config.toString())
      .set("headers", headerFields)
      .set("body", response);
    writer.emit(recordBuilder.build());
  }
}
