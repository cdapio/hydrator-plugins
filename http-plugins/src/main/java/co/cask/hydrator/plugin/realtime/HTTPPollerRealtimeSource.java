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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.ReferenceRealtimeSource;
import co.cask.hydrator.common.http.HTTPPollConfig;
import co.cask.hydrator.common.http.HTTPRequestor;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Real Time Source to poll and fetch data from a url.
 */
@Plugin(type = RealtimeSource.PLUGIN_TYPE)
@Name("HTTPPoller")
@Description("Fetch data by performing an HTTP request at a regular interval.")
public class HTTPPollerRealtimeSource extends ReferenceRealtimeSource<StructuredRecord> {
  private static final String POLL_TIME_STATE_KEY = "lastPollTime";

  private final HTTPPollConfig config;
  private HTTPRequestor httpRequestor;

  public HTTPPollerRealtimeSource(HTTPPollConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(HTTPRequestor.SCHEMA);
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    config.validate();
    httpRequestor = new HTTPRequestor(config);
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
      writer.emit(httpRequestor.get());
    } finally {
      currentState.setState(POLL_TIME_STATE_KEY, Bytes.toBytes(System.currentTimeMillis()));
    }
    return currentState;
  }
}
