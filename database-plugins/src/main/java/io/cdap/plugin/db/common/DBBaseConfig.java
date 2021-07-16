/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.db.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.db.DBUtils;
import io.cdap.plugin.db.connector.DBConnectorConfig;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Database source and sink can re-use
 */
public class DBBaseConfig extends PluginConfig {
  public static final String COLUMN_NAME_CASE = "columnNameCase";
  public static final String ENABLE_AUTO_COMMIT = "enableAutoCommit";
  public static final String NAME_USE_CONNECTION = "useConnection";
  public static final String NAME_CONNECTION = "connection";
  public static final String JDBC_PLUGIN_TYPE = "jdbcPluginType";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(COLUMN_NAME_CASE)
  @Description("Sets the case of the column names returned from the query. " +
    "Possible options are upper or lower. By default or for any other input, the column names are not modified and " +
    "the names returned from the database are used as-is. Note that setting this property provides predictability " +
    "of column name cases across different databases but might result in column name conflicts if multiple column " +
    "names are the same when the case is ignored.")
  @Nullable
  @Macro
  private String columnNameCase;

  @Name(ENABLE_AUTO_COMMIT)
  @Description("Whether to enable auto commit for queries run by this source. Defaults to false. " +
    "This setting should only matter if you are using a jdbc driver that does not support a false value for " +
    "auto commit, or a driver that does not support the commit call. For example, the Hive jdbc driver will throw " +
    "an exception whenever a commit is called. For drivers like that, this should be set to true.")
  @Nullable
  private Boolean enableAutoCommit;

  @Name(NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private DBConnectorConfig connection;

  @Name(JDBC_PLUGIN_TYPE)
  @Description("Type of the JDBC plugin to use. This is the value of the 'type' key defined in the JSON file " +
    "for the JDBC plugin. Defaults to 'jdbc'.")
  @Nullable
  public String jdbcPluginType;

  public boolean getEnableAutoCommit() {
    return enableAutoCommit == null ? false : enableAutoCommit;
  }

  public String getJdbcPluginType() {
    return jdbcPluginType == null ? DBUtils.PLUGIN_TYPE_JDBC : jdbcPluginType;
  }

  @Nullable
  protected String cleanQuery(@Nullable String query) {
    if (query == null) {
      return null;
    }
    query = query.trim();
    if (query.isEmpty()) {
      return query;
    }
    // find the last character that is not whitespace or a semicolon
    int idx = query.length() - 1;
    char currChar = query.charAt(idx);
    while (idx > 0 && currChar == ';' || Character.isWhitespace(currChar)) {
      idx--;
      currChar = query.charAt(idx);
    }
    // why would somebody do this?
    if (idx == 0) {
      return "";
    }
    return query.substring(0, idx + 1);
  }

  public String getJdbcPluginName() {
    return connection.getJdbcPluginName();
  }

  public Properties getAllConnectionArguments() {
    return connection.getConnectionArgumentsProperties();
  }

  public String getConnectionArguments() {
    return connection.getConnectionArguments();
  }

  public String getConnectionString() {
    return connection.getConnectionString();
  }

  @Nullable
  public String getUser() {
    return connection.getUser();
  }

  @Nullable
  public String getPassword() {
    return connection.getPassword();
  }

  public DBConnectorConfig getConnection() {
    return connection;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getColumnNameCase() {
    return columnNameCase;
  }

  public boolean getUseConnection() {
    return useConnection == null ? false : useConnection;
  }
}
