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

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Twitter")
@Description("Twitter streaming source.")
public class TwitterStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final String ID = "id";
  private static final String MSG = "message";
  private static final String LANG = "lang";
  private static final String TIME = "time";
  private static final String FAVC = "favCount";
  private static final String RTC = "rtCount";
  private static final String SRC = "source";
  private static final String GLAT = "geoLat";
  private static final String GLNG = "geoLong";
  private static final String ISRT = "isRetweet";

  private static final Schema SCHEMA =
    Schema.recordOf("tweet", Schema.Field.of(ID, Schema.of(Schema.Type.LONG)),
                    Schema.Field.of(MSG, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(LANG, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of(TIME, Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of(FAVC, Schema.of(Schema.Type.INT)),
                    Schema.Field.of(RTC, Schema.of(Schema.Type.INT)),
                    Schema.Field.of(SRC, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of(GLAT, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of(GLNG, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of(ISRT, Schema.of(Schema.Type.BOOLEAN)));

  private final TwitterStreamingConfig config;

  public TwitterStreamingSource(TwitterStreamingConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(SCHEMA);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    registerUsage(context);
    JavaStreamingContext javaStreamingContext = context.getSparkStreamingContext();

    // Create authorization from user-provided properties
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setDebugEnabled(false)
      .setOAuthConsumerKey(config.consumerKey)
      .setOAuthConsumerSecret(config.consumerSecret)
      .setOAuthAccessToken(config.accessToken)
      .setOAuthAccessTokenSecret(config.accessTokenSecret);
    Authorization authorization = new OAuthAuthorization(configurationBuilder.build());

    return TwitterUtils.createStream(javaStreamingContext, authorization).map(
      new Function<Status, StructuredRecord>() {
        public StructuredRecord call(Status status) {
          return convertTweet(status);
        }
      }
    );
  }

  private StructuredRecord convertTweet(Status tweet) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(SCHEMA);
    recordBuilder.set(ID, tweet.getId());
    recordBuilder.set(MSG, tweet.getText());
    recordBuilder.set(LANG, tweet.getLang());
    Date tweetDate = tweet.getCreatedAt();
    if (tweetDate != null) {
      recordBuilder.set(TIME, tweetDate.getTime());
    }
    recordBuilder.set(FAVC, tweet.getFavoriteCount());
    recordBuilder.set(RTC, tweet.getRetweetCount());
    recordBuilder.set(SRC, tweet.getSource());
    if (tweet.getGeoLocation() != null) {
      recordBuilder.set(GLAT, tweet.getGeoLocation().getLatitude());
      recordBuilder.set(GLNG, tweet.getGeoLocation().getLongitude());
    }
    recordBuilder.set(ISRT, tweet.isRetweet());
    return recordBuilder.build();
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
  }

}
