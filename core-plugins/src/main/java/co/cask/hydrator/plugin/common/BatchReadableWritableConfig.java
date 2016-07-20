/*
 * Copyright 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

/**
 * Config classes for batch readable writable sources/sinks.
 */
public class BatchReadableWritableConfig extends PluginConfig {
  @Name(Properties.BatchReadableWritable.NAME)
  @Description("Name of the dataset. If the dataset does not already exist, one will be created.")
  @Macro
  String name;

  public BatchReadableWritableConfig(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
