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

import javax.annotation.Nullable;

/**
 * Properties for the {@link AbstractFileSink}.
 *
 * This is an interface and not an abstract class in case plugin implementations do not wish to support all of the
 * properties the AbstractFileSource supports, in case they want to use different descriptions in their
 * PluginConfig, or in case there is some class hierarchy that does not allow it.
 */
public interface FileSinkProperties {

  /**
   * Validates the properties.
   *
   * @throws IllegalArgumentException if anything is invalid
   * Deprecated since 2.3.0. Use {@link FileSinkProperties#validate(FailureCollector)} method instead.
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
   * Get the name that will be used to identify the sink for lineage and metadata.
   */
  String getReferenceName();

  /**
   * Get the path to write to.
   */
  String getPath();

  /**
   * Get the format of the data to write.
   */
  FileFormat getFormat();

  /**
   * Get the output schema if it is known and constant, or null if it is not known or not constant.
   */
  @Nullable
  Schema getSchema();

  /**
   * Get the time format for the output directory that will be appended to the path.
   * For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
   * If not specified, nothing will be appended to the path.
   */
  @Nullable
  String getSuffix();
}
