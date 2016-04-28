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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.config.HTTPConfig;
import com.google.common.base.Charsets;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import javax.annotation.Nullable;

/**
 * Config class for HTTP Poll plugin.
 */
public class HTTPPollConfig extends HTTPConfig {
  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  public String referenceName;

  @Description("The amount of time to wait between each poll in seconds.")
  private long interval;

  @Description("The charset used to decode the response. Defaults to UTF-8.")
  @Nullable
  private String charset;

  @Description("Sets the read timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute).")
  @Nullable
  private Integer readTimeout;

  public HTTPPollConfig() {
    this("", null, 60);
  }

  public HTTPPollConfig(String referenceName, String url, long interval) {
    this(referenceName, url, interval, null);
  }

  public HTTPPollConfig(String referenceName, String url, long interval, String requestHeaders) {
    super(url, requestHeaders);
    this.interval = interval;
    this.charset = Charsets.UTF_8.name();
    this.readTimeout = 60 * 1000;
    this.referenceName = referenceName;
  }

  public long getInterval() {
    return interval;
  }

  public Charset getCharset() {
    return Charset.forName(charset);
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  @SuppressWarnings("ConstantConditions")
  public void validate() {
    super.validate();
    if (interval <= 0) {
      throw new IllegalArgumentException(String.format(
        "Invalid interval %d. Interval must be greater than 0.", interval));
    }
    if (readTimeout < 0) {
      throw new IllegalArgumentException(String.format(
        "Invalid readTimeout %d. Timeout must be 0 or a positive number.", readTimeout));
    }
    try {
      Charset.forName(charset);
    } catch (UnsupportedCharsetException e) {
      throw new IllegalArgumentException(String.format("Invalid charset %s.", charset));
    }
  }
}
