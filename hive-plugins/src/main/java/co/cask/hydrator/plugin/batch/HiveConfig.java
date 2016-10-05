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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Hive batch plugins
 */
public class HiveConfig extends ReferencePluginConfig {

  @Macro
  @Name(Hive.METASTORE_URI)
  @Description("The URI of Hive metastore in the following format: thrift://<hostname>:<port>. " +
    "Example: thrift://somehost.net:9083")
  public String metaStoreURI;

  @Macro
  @Name(Hive.DB_NAME)
  @Description("The name of the database. Defaults to 'default'.")
  @Nullable
  public String dbName;

  @Macro
  @Name(Hive.TABLE_NAME)
  @Description("The name of the table. This table must exist.")
  public String tableName;

  public HiveConfig() {
    super(String.format("Hive"));
    dbName = "default";
  }

  /**
   * Hive config variables
   */
  public static class Hive {
    public static final String METASTORE_URI = "metastoreURI";
    public static final String DB_NAME = "databaseName";
    public static final String TABLE_NAME = "tableName";
    public static final String PARTITIONS = "partitions";
    public static final String SCHEMA = "schema";
  }
}
