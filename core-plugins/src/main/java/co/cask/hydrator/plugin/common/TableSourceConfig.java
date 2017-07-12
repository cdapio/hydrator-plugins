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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.plugin.batch.sink.TableSink;
import co.cask.hydrator.plugin.batch.source.TableSource;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for {@link TableSource}, {@link TableSink}
 */
public class TableSourceConfig extends BatchReadableWritableConfig {
  @Macro
  @Name(Properties.Table.PROPERTY_SCHEMA)
  @Description("Schema of the table as a JSON Object. If the table does not already exist, one will be " +
    "created with this schema, which will allow the table to be explored through Hive.")
  private String schemaStr;

  @Name(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD)
  @Description("Optional record field for which row key will be considered as value instead of row column. " +
    "The field name specified must be present in the schema, and must not be nullable.")
  @Nullable
  @Macro
  private String rowField;

  public TableSourceConfig(String name, String rowField, @Nullable String schemaStr) {
    super(name);
    this.rowField = rowField;
    this.schemaStr = schemaStr;
  }

  @Nullable
  public String getSchemaStr() {
    return schemaStr;
  }

  public String getRowField() {
    return rowField;
  }

  @Nullable
  public Schema getSchema() {
    if (!containsMacro(Properties.Table.PROPERTY_SCHEMA)) {
      try {
        return Schema.parseJson(schemaStr);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }
    return null;
  }
}
