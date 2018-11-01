/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.IdUtils;
import co.cask.hydrator.format.FileFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import javax.annotation.Nullable;

/**
 * Sink configuration.
 */
@SuppressWarnings("unused")
public abstract class AbstractFileSinkConfig extends PluginConfig implements FileSinkProperties {
  @Description("Name be used to uniquely identify this sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Macro
  @Nullable
  @Description("The time format for the output directory that will be appended to the path. " +
    "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
    "If not specified, nothing will be appended to the path.")
  private String suffix;

  @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
    + "or 'delimited'.")
  private String format;

  @Macro
  @Nullable
  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
    + "is anything other than 'delimited'.")
  private String delimiter;

  @Macro
  @Nullable
  @Description("The schema of the data to write.")
  private String schema;

  public void validate() {
    IdUtils.validateId(referenceName);
    if (suffix != null && !containsMacro("suffix")) {
      new SimpleDateFormat(suffix);
    }
    if (!containsMacro("format")) {
      getFormat();
    }
    getSchema();
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }

  @Override
  @Nullable
  public String getSuffix() {
    return suffix;
  }

  @Nullable
  public Schema getSchema() {
    if (containsMacro("schema") || schema == null) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
    }
  }

  /**
   * Logically equivalent to valueOf except it throws an exception with a message that indicates what the valid
   * enum values are.
   */
  @Override
  public FileFormat getFormat() {
    return FileFormat.from(format, FileFormat::canWrite);
  }
}
