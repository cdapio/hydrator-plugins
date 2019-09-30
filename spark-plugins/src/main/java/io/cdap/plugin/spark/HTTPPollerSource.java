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

package io.cdap.plugin.spark;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.http.HTTPPollConfig;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Polls a http endpoints and outputs a record for each url response.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("HTTPPoller")
@Description("Fetch data by performing an HTTP request at a regular interval.")
public class HTTPPollerSource extends StreamingSource<StructuredRecord> {
  private final HTTPPollConfig conf;

  public HTTPPollerSource(HTTPPollConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    conf.validate(collector);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {
    FailureCollector collector = streamingContext.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    return HTTPPollerSourceUtil.getJavaDStream(streamingContext, conf);
  }
}
