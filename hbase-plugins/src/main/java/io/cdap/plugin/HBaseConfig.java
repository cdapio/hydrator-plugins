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

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.sink.HBaseSink;
import io.cdap.plugin.source.HBaseSource;

import java.io.IOException;
import javax.annotation.Nullable;

/**
* Base HBase Config for use in {@link HBaseSource} and {@link HBaseSink}.
*/
public class HBaseConfig extends ReferencePluginConfig {
  @Description("Name of the HBase Table")
  @Macro
  public String tableName;

  @Description("Name of the Column Family")
  @Macro
  public String columnFamily;

  @Description("Schema of the Record to be emitted (in case of Source) or received (in case of Sink)")
  public String schema;

  @Description("Field in the Schema that corresponds to row key")
  public String rowField;

  @Description("Zookeeper Quorum. By default it is set to 'localhost'")
  @Nullable
  @Macro
  public String zkQuorum;

  @Description("Zookeeper Client Port. By default it is set to 2181")
  @Nullable
  @Macro
  public String zkClientPort;

  public HBaseConfig(String referenceName, String tableName, String rowField, @Nullable String schema) {
    super(referenceName);
    this.tableName = tableName;
    this.rowField = rowField;
    this.schema = schema;
  }

  /**
   * @return {@link Schema} of the dataset if one was given
   * @throws IllegalArgumentException if the schema is null or not as valid JSON
   */
  public Schema getSchema() {
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null");
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
                                                       schema, e.getMessage()), e);
    }
  }
}
