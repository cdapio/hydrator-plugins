/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.TimeParser;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Config for TimePartitionedFileSet sources
 */
@SuppressWarnings("unused")
public class TPFSConfig extends PluginConfig {
  @Macro
  @Description("Name of the TimePartitionedFileSet to read.")
  private String name;

  @Macro
  @Nullable
  @Description("Base path for the TimePartitionedFileSet. Defaults to the name of the dataset.")
  private String basePath;

  @Macro
  @Description("Size of the time window to read with each run of the pipeline. " +
    "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
    "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of '5m' means each run of " +
    "the pipeline will read 5 minutes of events from the TPFS source.")
  private String duration;

  @Macro
  @Nullable
  @Description("Optional delay for reading from TPFS source. The value must be " +
    "of the same format as the duration value. For example, a duration of '5m' and a delay of '10m' means each run " +
    "of the pipeline will read 5 minutes of data from 15 minutes before its logical start time to 10 minutes " +
    "before its logical start time. The default value is 0.")
  private String delay;

  @Description("Schema of the data to read.")
  private String schema;

  public String getName() {
    return name;
  }

  @Nullable
  public String getBasePath() {
    return basePath;
  }

  public String getDuration() {
    return duration;
  }

  @Nullable
  public String getDelay() {
    return delay;
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
    }
  }

  public void validate() {
    // check duration and delay
    if (!containsMacro("duration")) {
      long durationInMs = TimeParser.parseDuration(duration);
      Preconditions.checkArgument(durationInMs > 0, "Duration must be greater than 0");
    }
    if (!containsMacro("delay") && !Strings.isNullOrEmpty(delay)) {
      TimeParser.parseDuration(delay);
    }
    getSchema();
  }
}

