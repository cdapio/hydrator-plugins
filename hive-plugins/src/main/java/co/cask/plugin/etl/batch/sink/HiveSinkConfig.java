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

package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.plugin.etl.batch.HiveConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Hive batch sink
 */
public class HiveSinkConfig extends HiveConfig {
  @Name(Hive.FILTER)
  @Description("Hive expression filter for write provided as a JSON Map of key value pairs that describe all of the " +
    "partition keys and values for that partition. For example if the partition column is 'type' then this property " +
    "should specified as:" +
    "\"{\"type\":\"typeOne\"}\"." +
    "To write multiple partitions simultaneously you can leave this empty, but all of the partitioning columns must " +
    "be present in the data you are writing to the sink.")
  @Nullable
  public String filter;

  @Name(Hive.SCHEMA)
  @Description("Optional schema to use while writing to Hive table. If no schema is provided then the " +
    "schema of the table will be used and it should match the schema of the data being written.")
  @Nullable
  public String schema;
}
