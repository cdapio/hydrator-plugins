/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.output;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Base config for delimited text formats.
 */
public class DelimitedPluginConfig extends PluginConfig {
  protected static final String HEADER_DESC = "Whether to write a header to each output file.";

  @Macro
  @Nullable
  @Description(HEADER_DESC)
  private Boolean writeHeader;

  public boolean shouldWriteHeader() {
    return writeHeader == null ? false : writeHeader;
  }
}
