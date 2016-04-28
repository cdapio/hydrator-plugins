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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.sink.HBaseSink;
import co.cask.hydrator.plugin.source.HBaseSource;

import javax.annotation.Nullable;

/**
* Base HBase Config for use in {@link HBaseSource} and {@link HBaseSink}.
*/
public class HBaseConfig extends ReferencePluginConfig {
  @Description("Name of the HBase Table")
  public String tableName;

  @Description("Name of the Column Family")
  public String columnFamily;

  @Description("Schema of the Record to be emitted (in case of Source) or received (in case of Sink)")
  public String schema;

  @Description("Field in the Schema that corresponds to row key")
  public String rowField;

  @Description("Zookeeper Quorum. By default it is set to 'localhost'")
  @Nullable
  public String zkQuorum;

  @Description("Zookeeper Client Port. By default it is set to 2181")
  @Nullable
  public String zkClientPort;

  public HBaseConfig(String referenceName, String tableName, String rowField, @Nullable String schema) {
    super(referenceName);
    this.tableName = tableName;
    this.rowField = rowField;
    this.schema = schema;
  }
}
