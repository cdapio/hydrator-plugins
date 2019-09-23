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

package io.cdap.plugin.common.http;

import com.google.common.base.Charsets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import javax.annotation.Nullable;

/**
 * Config class for HTTP Poll plugin.
 */
public class HTTPPollConfig extends HTTPConfig {
  private static final String NAME_INTERVAL = "interval";
  private static final String NAME_READ_TIMEOUT = "readTimeout";
  private static final String NAME_CHARSET = "charset";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  public String referenceName;

  @Name(NAME_INTERVAL)
  @Description("The amount of time to wait between each poll in seconds.")
  private long interval;

  @Name(NAME_CHARSET)
  @Description("The charset used to decode the response. Defaults to UTF-8.")
  @Nullable
  private String charset;

  @Name(NAME_READ_TIMEOUT)
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

  public void validate(FailureCollector collector) {
    if (!containsMacro(NAME_INTERVAL) && interval <= 0) {
      collector.addFailure(String.format("Invalid interval '%d'.", interval),
                           "Interval must be greater than 0.").withConfigProperty(NAME_INTERVAL);
    }

    if (readTimeout < 0) {
      collector.addFailure(String.format("Invalid readTimeout '%d'.", readTimeout),
                           "Timeout must be 0 or a positive number.").withConfigProperty(NAME_READ_TIMEOUT);
    }

    try {
      Charset.forName(charset);
    } catch (UnsupportedCharsetException e) {
      collector.addFailure(String.format("Invalid charset '%s'.", charset),
                           "Supported character sets are : ISO-8859-1, US-ASCII, UTF-8, UTF-16, UTF-16BE, UTF-16LE")
        .withConfigProperty(NAME_CHARSET);
    }
  }
}
