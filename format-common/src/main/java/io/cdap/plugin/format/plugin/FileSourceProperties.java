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

package io.cdap.plugin.format.plugin;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.format.FileFormat;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Properties for the {@link AbstractFileSource}.
 *
 * This is an interface and not an abstract class in case plugin implementations do not wish to support all of the
 * properties the AbstractFileSource supports, in case they want to use different descriptions in their
 * PluginConfig, or in case there is some class hierarchy that does not allow it.
 */
public interface FileSourceProperties {
  String PATH_FIELD = "pathField";

  /**
   * Validates the properties.
   *
   * @throws IllegalArgumentException if anything is invalid
   * Deprecated since 2.3.0. Use {@link FileSourceProperties#validate(FailureCollector)} method instead.
   */
  @Deprecated
  void validate();

  /**
   * Validates the properties and collects validation failures if anything is invalid.
   *
   * @param collector failure collector
   */
  default void validate(FailureCollector collector) {
    // no-op
  }

  /**
   * Get the name that will be used to identify the source for lineage and metadata.
   */
  String getReferenceName();

  /**
   * Get the path to read from.
   */
  String getPath();

  /**
   * Get the format of the data to read.
   */
  FileFormat getFormat();

  /**
   * Get the pattern that file names must match if filename filter should be done.
   */
  @Nullable
  Pattern getFilePattern();

  /**
   * Get the maximum size in bytes for an input split.
   */
  long getMaxSplitSize();

  /**
   * Whether to allow a path that doesn't exist.
   */
  boolean shouldAllowEmptyInput();

  /**
   * Whether to read the input path recursively.
   */
  boolean shouldReadRecursively();

  /**
   * The output field to place the file path that the record was read from, if path tracking should be done.
   */
  @Nullable
  String getPathField();

  /**
   * Whether to only use the filename rather than the entire URI of the file path,
   * if {@link #getPathField()} is present.
   */
  boolean useFilenameAsPath();

  /**
   * The output schema if it is known and constant.
   */
  @Nullable
  Schema getSchema();
}
