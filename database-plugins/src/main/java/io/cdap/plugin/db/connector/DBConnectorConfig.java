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

package io.cdap.plugin.db.connector;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.KeyValueListParser;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source, sink, and action can all re-use.
 */
public class DBConnectorConfig extends PluginConfig {
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String CONNECTION_ARGUMENTS = "connectionArguments";
  public static final String JDBC_PLUGIN_NAME = "jdbcPluginName";

  @Name(CONNECTION_STRING)
  @Description("JDBC connection string. May include database name.")
  @Macro
  private String connectionString;

  @Name(USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String user;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  private String password;

  @Name(CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string tag/value pairs as connection arguments. This is a semicolon-separated " +
    "list of key-value pairs, where each pair is separated by a equals '=' and specifies the key and value for the " +
    "argument. For example, 'key1=value1;key2=value' specifies that the connection will be given arguments 'key1' " +
    "mapped to 'value1' and the argument 'key2' mapped to 'value2'.")
  @Nullable
  @Macro
  private String connectionArguments;

  @Name(JDBC_PLUGIN_NAME)
  @Description("Name of the JDBC plugin to use. This is the value of the 'name' key defined in the JSON file " +
    "for the JDBC plugin.")
  private String jdbcPluginName;

  public DBConnectorConfig(String user, String password, String jdbcPluginName, String connectionString,
    String connectionArguments) {
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionString = connectionString;
    this.connectionArguments = connectionArguments;
  }


  /**
   * Parses connection arguments into a {@link Properties}.
   *
   * @param connectionArguments See {@link DBConnectorConfig#connectionArguments}.
   * @param user See {@link DBConnectorConfig#user}.
   * @param password See {@link DBConnectorConfig#password}.
   */
  public static Properties getAllConnectionArguments(@Nullable String connectionArguments,
                                                  @Nullable String user, @Nullable String password) {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*;\\s*", "=");

    Map<String, String> connectionArgumentsMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(connectionArguments)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(connectionArguments)) {
        connectionArgumentsMap.put(keyVal.getKey(), keyVal.getValue());
      }
    }

    if (user != null) {
      connectionArgumentsMap.put("user", user);
    }
    if (password != null) {
      connectionArgumentsMap.put("password", password);
    }
    Properties properties = new Properties();
    properties.putAll(connectionArgumentsMap);
    return properties;
  }

  /**
   * @return a {@link Properties} of connection arguments, parsed from the config including the username and password.
   */
  public Properties getAllConnectionArguments() {
    return getAllConnectionArguments(connectionArguments, user, password);
  }

  public String getConnectionArguments() {
    return connectionArguments;
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

}
