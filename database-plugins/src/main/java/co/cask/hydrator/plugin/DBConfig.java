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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source and sink can re-use
 */
public class DBConfig extends ConnectionConfig {

  @Description("Sets the case of the column names returned from the query. " +
    "Possible options are upper or lower. By default or for any other input, the column names are not modified and " +
    "the names returned from the database are used as-is. Note that setting this property provides predictability " +
    "of column name cases across different databases but might result in column name conflicts if multiple column " +
    "names are the same when the case is ignored.")
  @Nullable
  public String columnNameCase;

  @Description("Name of the database table to use as a source or a sink.")
  public String tableName;
}
