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

package co.cask.hydrator.plugin.realtime.config;

/**
 * Config class for Url Fetch plugin
 */

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

/**
 * Config class for Source.
 */
public class UrlFetchRealtimeSourceConfig extends PluginConfig {

  @Name("URL")
  @Description("The url to fetch data from.")
  private String url;

  @Name("Delay")
  @Description("The amount of time to wait between each poll in seconds.")
  private long delayInSeconds;

  public UrlFetchRealtimeSourceConfig(String url, long delayInSeconds) {
    this.url = url;
    this.delayInSeconds = delayInSeconds;
  }

  public String getUrl() {
    return url;
  }

  public long getDelayInSeconds() {
    return delayInSeconds;
  }
}
