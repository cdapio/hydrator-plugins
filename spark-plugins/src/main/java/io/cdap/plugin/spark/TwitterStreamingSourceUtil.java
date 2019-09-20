/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Date;

/**
 * Util method for {@link TwitterStreamingSource}.
 *
 * This class contains methods for {@link TwitterStreamingSource} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
class TwitterStreamingSourceUtil {
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

  /**
   * Returns {@link JavaDStream} for {@link TwitterStreamingSource}.
   *
   * @param context streaming context
   * @param config twitter streaming source config
   */
  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context,
                                                      TwitterStreamingSource.TwitterStreamingConfig config) {
    // Create authorization from user-provided properties
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setDebugEnabled(false)
      .setOAuthConsumerKey(config.getConsumerKey())
      .setOAuthConsumerSecret(config.getConsumerSecret())
      .setOAuthAccessToken(config.getAccessToken())
      .setOAuthAccessTokenSecret(config.getAccessTokenSecret());
    Authorization authorization = new OAuthAuthorization(configurationBuilder.build());

    return TwitterUtils.createStream(context.getSparkStreamingContext(), authorization).map(
      (Function<Status, StructuredRecord>) TwitterStreamingSourceUtil::convertTweet
    );
  }

  private static StructuredRecord convertTweet(Status tweet) {
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

  static Schema getTwitterSourceSchema() {
    return SCHEMA;
  }

  private TwitterStreamingSourceUtil() {
    // no-op
  }
}
