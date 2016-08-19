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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source, sink, and action can all re-use.
 */
public class ConnectionConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String JDBC_PLUGIN_NAME = "jdbcPluginName";
  public static final String JDBC_PLUGIN_TYPE = "jdbcPluginType";
  public static final String COLUMN_NAME_CASE = "columnNameCase";
  public static final String ENABLE_AUTO_COMMIT = "enableAutoCommit";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string including database name.")
  @Macro
  public String connectionString;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  @Name(JDBC_PLUGIN_NAME)
  @Description("Name of the JDBC plugin to use. This is the value of the 'name' key defined in the JSON file " +
    "for the JDBC plugin.")
  public String jdbcPluginName;

  @Name(JDBC_PLUGIN_TYPE)
  @Description("Type of the JDBC plugin to use. This is the value of the 'type' key defined in the JSON file " +
    "for the JDBC plugin. Defaults to 'jdbc'.")
  @Nullable
  public String jdbcPluginType;

  @Name(ENABLE_AUTO_COMMIT)
  @Description("Whether to enable auto commit for queries run by this source. Defaults to false. " +
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
