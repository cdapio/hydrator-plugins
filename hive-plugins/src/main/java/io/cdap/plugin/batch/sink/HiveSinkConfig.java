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

package io.cdap.plugin.batch.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.batch.HiveConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Configurations for Hive batch sink
 */
public class HiveSinkConfig extends HiveConfig {
  @Name(Hive.PARTITIONS)
  @Description("A JSON Map of key value pairs that describe all of the partition keys and values for the partition " +
    "to be written. For example if the partition column is 'type' then this property " +
    "should specified as:" +
    "\"{\"type\":\"typeOne\"}\"." +
    "To write multiple partitions simultaneously you can leave this empty, but all of the partitioning columns must " +
    "be present in the data you are writing to the sink.")
  @Nullable
  public String partitions;

  @Name(Hive.SCHEMA)
  @Description("Optional schema to use while writing to Hive table. If no schema is provided then the " +
    "schema of the table will be used and it should match the schema of the data being written.")
  @Nullable
  public String schema;

  /**
   * @return {@link Schema} of the dataset if one was given else null
   */
  @Nullable
  public Schema getSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                       schema, e.getMessage()), e);
    }
  }
}
