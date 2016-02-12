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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.MacroConfig;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source, sink, and action can all re-use.
 */
public class ConnectionConfig extends MacroConfig {

  @Description("JDBC connection string including database name.")
  public String connectionString;

  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String user;

  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String password;

  @Description("Name of the JDBC plugin to use. This is the value of the 'name' key defined in the JSON file " +
    "for the JDBC plugin.")
  public String jdbcPluginName;

  @Description("Type of the JDBC plugin to use. This is the value of the 'type' key defined in the JSON file " +
    "for the JDBC plugin. Defaults to 'jdbc'.")
  @Nullable
  public String jdbcPluginType;

  @Description("Whether to enable auto commit for queries run by this plugin. Defaults to false. " +
    "This setting should only matter if you are using a jdbc driver that does not support a false value for " +
    "auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw " +
    "an exception whenever a commit is called. For drivers like that, this should be set to true.")
  @Nullable
  public Boolean enableAutoCommit;

  public ConnectionConfig() {
    jdbcPluginType = "jdbc";
    enableAutoCommit = false;
  }
}
