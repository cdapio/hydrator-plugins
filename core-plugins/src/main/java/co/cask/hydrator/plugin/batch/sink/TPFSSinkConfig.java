/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.TimeParser;
import com.google.common.base.Strings;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Abstract config for TimePartitionedFileSetSink
 */
public abstract class TPFSSinkConfig extends PluginConfig {

  @Macro
  @Description("The schema of the FileSet.")
  protected String schema;

  @Description("Name of the Time Partitioned FileSet Dataset to which the records " +
    "are written to. If it doesn't exist, it will be created.")
  @Macro
  protected String name;

  @Description("The base path for the Time Partitioned FileSet. Defaults to the " +
    "name of the dataset.")
  @Nullable
  @Macro
  protected String basePath;

  @Description("The format for the path; for example: " +
    "'yyyy-MM-dd/HH-mm' will create a file path ending such as '2015-01-01/20-42'. " +
    "The string provided will be passed to SimpleDataFormat. " +
    "If left blank, the partitions will be of the form '2015-01-01/20-42.142017372000'. " +
    "Note that each partition must have a unique file path or a runtime exception will be thrown.")
  @Nullable
  @Macro
  protected String filePathFormat;

  @Description("The time zone to format the partition. " +
    "This option is only used if filePathFormat is set. If blank or an invalid TimeZone ID, defaults to UTC. " +
    "Note that the time zone provided must be recognized by TimeZone.getTimeZone(String); " +
    "for example: \"America/Los_Angeles\"")
  @Nullable
  @Macro
  protected String timeZone;

  @Description("Amount of time to subtract from the pipeline runtime to determine the output partition. " +
    "Defaults to 0m. The format is expected to be a number followed by an 's', 'm', 'h', or 'd' " +
    "specifying the time unit, with 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. " +
    "For example, if the pipeline is scheduled to run at midnight of January 1, 2016, and the offset is set to '1d', " +
    "data will be written to the partition for midnight Dec 31, 2015.")
  @Nullable
  @Macro
  protected String partitionOffset;

  @Description("Optional property that configures the sink to delete old partitions after successful runs. " +
    "If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and " +
    "delete any partitions older than that time. " +
    "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
    "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to " +
    "run at midnight of January 1, 2016, and this property is set to 7d, the sink will delete any partitions " +
    "for time partitions older than midnight Dec 25, 2015.")
  @Nullable
  @Macro
  protected String cleanPartitionsOlderThan;

  public TPFSSinkConfig(String name, @Nullable String basePath,
                        @Nullable String filePathFormat, @Nullable String timeZone) {
    this.name = name;
    this.basePath = basePath;
    this.filePathFormat = filePathFormat;
    this.timeZone = timeZone;
  }

  public void validate() {
    if (!containsMacro("filePathFormat") && !Strings.isNullOrEmpty(timeZone) && Strings.isNullOrEmpty(filePathFormat)) {
      throw new IllegalArgumentException("The filePathFormat setting must be set in order to set timeZone.");
    }

    // no macro checks necessary as at config time, string properties configured with macros are null
    if (partitionOffset != null) {
      TimeParser.parseDuration(partitionOffset);
    }
    if (cleanPartitionsOlderThan != null) {
      long oldTime = TimeParser.parseDuration(cleanPartitionsOlderThan);
      if (oldTime < TimeUnit.MINUTES.toMillis(1)) {
        throw new IllegalArgumentException("Cannot clean partitions from less than 1 minute ago.");
      }
    }
    if (schema != null) {
      getSchema();
    }
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not parse schema: " + e.getMessage());
    }
  }
}
