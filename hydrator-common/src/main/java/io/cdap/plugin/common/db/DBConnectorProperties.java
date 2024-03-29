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
 *
 */

package io.cdap.plugin.common.db;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Properties for the {@link AbstractDBConnector}.
 */
public interface DBConnectorProperties {
  String PLUGIN_JDBC_PLUGIN_NAME = "jdbcPluginName";

  /**
   * Get the connection string of this database connector
   */
  String getConnectionString();

  /**
   * Get the user name for the connection string
   */
  @Nullable
  String getUser();

  /**
   * Get the password for the connection string
   */
  @Nullable
  String getPassword();

  /**
   * @return a {@link Properties} of connection arguments, including the username and password.
   */
  Properties getConnectionArgumentsProperties();

  /**
   * Get the jdbc plugin name
   */
  String getJdbcPluginName();

  /**
   * @return a string of connection arguments in form of key1=val1;key2=val2...
   */
  String getConnectionArguments();
}
