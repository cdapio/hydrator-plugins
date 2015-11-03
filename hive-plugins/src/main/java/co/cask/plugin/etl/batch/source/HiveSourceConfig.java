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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.plugin.etl.batch.HiveConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Hive batch source
 */
public class HiveSourceConfig extends HiveConfig {
  @Name(Hive.FILTER)
  @Description("Hive expression filter for scan. This filter must reference only partition columns. " +
    "Values from other columns will cause the pipeline to fail.")
  @Nullable
  public String filter;

  @Name(Hive.SCHEMA)
  @Description("Optional schema to use while reading from Hive table. If no schema is provided then the " +
    "schema of the source table will be used. Note: If you want to use a hive table which has non-primitive types as " +
    "a source then you should provide a schema here with non-primitive fields dropped else your pipeline will fail.")
  @Nullable
  public String schema;
}
