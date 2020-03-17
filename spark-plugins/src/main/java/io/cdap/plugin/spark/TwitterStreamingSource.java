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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * Twitter streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Twitter")
@Description("Twitter streaming source.")
public class TwitterStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private final TwitterStreamingConfig config;

  public TwitterStreamingSource(TwitterStreamingConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(TwitterStreamingSourceUtil.getTwitterSourceSchema());
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = TwitterStreamingSourceUtil.getTwitterSourceSchema();
    // record dataset lineage
    context.registerLineage(config.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from twitter",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    return TwitterStreamingSourceUtil.getJavaDStream(context, config);
  }

  /**
   * Config class for TwitterStreamingSource.
   */
  public static class TwitterStreamingConfig extends ReferencePluginConfig implements Serializable {

    private static final long serialVersionUID = 4218063781909515444L;

    @Name("ConsumerKey")
    @Description("Consumer Key")
    @Macro
    private String consumerKey;

    @Name("ConsumerSecret")
    @Description("Consumer Secret")
    @Macro
    private String consumerSecret;

    @Name("AccessToken")
    @Description("Access Token")
    @Macro
    private String accessToken;

    @Name("AccessTokenSecret")
    @Description("Access Token Secret")
    @Macro
    private String accessTokenSecret;

    @VisibleForTesting
    public TwitterStreamingConfig(String referenceName, String consumerKey, String consumerSecret, String accessToken,
                                  String accessTokenSecret) {
      super(referenceName);
      this.consumerKey = consumerKey;
      this.consumerSecret = consumerSecret;
      this.accessToken = accessToken;
      this.accessTokenSecret = accessTokenSecret;
    }

    public static long getSerialVersionUID() {
      return serialVersionUID;
    }

    String getConsumerKey() {
      return consumerKey;
    }

    String getConsumerSecret() {
      return consumerSecret;
    }

    String getAccessToken() {
      return accessToken;
    }

    String getAccessTokenSecret() {
      return accessTokenSecret;
    }
  }
}
