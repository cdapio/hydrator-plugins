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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.etl.batch.sink.TPFSSinkConfig;


import javax.annotation.Nullable;

/**
 * Configuration for Vertica Plugin
 */
public class VerticaBulkLoadConfig extends TPFSSinkConfig {

  @Name(Vertica.DB_CONNECTION_URL)
  @Description("The connection URL to vertica host through JDBC with the database to connect to. " +
    "Example: jdbc:vertica://VerticaHost:5433/ExampleDB")
  @Nullable
  public String dbConnectionURL;

  @Name(Vertica.USER)
  @Description("The user, which must be the database superuser to perform the bulk load.")
  @Nullable
  public String user;

  @Name(Vertica.PASSWORD)
  @Description("The password for the given superuser")
  @Nullable
  public String password;

  @Name(Vertica.TABLE_NAME)
  @Description("The name of the table to copy data to")
  @Nullable
  public String tableName;

  public VerticaBulkLoadConfig(String name, @Nullable String basePath, @Nullable String filePathFormat,
                               @Nullable String timeZone) {
    super(name, basePath, filePathFormat, timeZone);
  }


  // getters for members variables in TPFSSinkConfig which has protected access
  public String getName() {
    return name;
  }

  public String getBasePath() {
    return basePath;
  }

  public String getFilePathFormat() {
    return filePathFormat;
  }

  public String timeZone() {
    return timeZone;
  }

  /**
   * Vertica config variables
   */
  public static class Vertica {
    public static final String DB_CONNECTION_URL = "dbConnectionURL";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String TABLE_NAME = "tableName";
  }
}
