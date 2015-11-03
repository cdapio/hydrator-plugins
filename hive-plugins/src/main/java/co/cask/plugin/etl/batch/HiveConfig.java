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

package co.cask.plugin.etl.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Hive batch plugins
 */
public class HiveConfig extends PluginConfig {

  @Name(Hive.METASTORE_URI)
  @Description("The URI of Hive metastore.")
  public String metaStoreURI;

  @Name(Hive.DB_NAME)
  @Description("The name of the database. Defaults to 'default'.")
  @Nullable
  public String dbName;

  @Name(Hive.TABLE_NAME)
  @Description("The name of the table.")
  public String tableName;

  public static class Hive {
    public static final String METASTORE_URI = "metastoreURI";
    public static final String DB_NAME = "databaseName";
    public static final String TABLE_NAME = "tableName";
    public static final String FILTER = "filter";
    public static final String SCHEMA = "schema";
  }
}
