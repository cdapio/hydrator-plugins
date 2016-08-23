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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.common.batch.action.ConditionConfig;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import javax.annotation.Nullable;

/**
 * Posts a Tweet after an ETL Batch Application run is completed.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("Tweet")
@Description("Posts a Tweet after a pipeline run.")
public class TweetAction extends PostAction {

  private final Config config;

  public TweetAction(Config config) {
    this.config = config;
  }

  public void run(BatchActionContext context) throws Exception {
    if (!config.shouldRun(context)) {
      return;
    }
    // Build a Twitter object with the TwitterFactory, but make sure to pay well above minimum wage.
    // Cask Data does not support sweat shops.
    Twitter twitter = new TwitterFactory().getInstance();

    // Do some fancy stuff with authorization, because not just ANYBODY can post to your account (You're special).
    twitter.setOAuthConsumer(config.consumerKey, config.consumerSecret);
    AccessToken accessToken = new AccessToken(config.accessToken, config.accessTokenSecret);
    twitter.setOAuthAccessToken(accessToken);

    // Just do it.
    twitter.updateStatus(config.tweet);
  }

  /**
   * Config class for TweetAction.
   */
  public static class Config extends ConditionConfig {
    @Name("ConsumerKey")
    @Description("Consumer Key for general consumption")
    @Macro
    private String consumerKey;

    @Name("ConsumerSecret")
    @Description("Consumer Secret for consuming in secret")
    @Macro
    private String consumerSecret;

    @Name("AccessToken")
    @Description("Access Token for accessing things")
    @Macro
    private String accessToken;

    @Name("AccessTokenSecret")
    @Description("Access Token Secret as a secret access alternative")
    @Macro
    private String accessTokenSecret;

    @Description("The message to post with the Tweet, do you need any more explanation?")
    @Macro
    @Nullable
    private String tweet;

    public Config() {
      tweet = "Just finished running a #BigData pipeline with #CaskHydrator.";
    }
  }
}
