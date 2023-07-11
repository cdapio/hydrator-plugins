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
 */

package io.cdap.plugin.format.delimited.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.format.input.PathTrackingConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common config for delimited related formats.
 */
public class DelimitedConfig extends PathTrackingConfig {

  // properties
  public static final String NAME_ENABLE_QUOTES_VALUES = "enableQuotedValues";
  public static final String NAME_OVERRIDE = "override";
  public static final String NAME_SAMPLE_SIZE = "sampleSize";
  public static final String NAME_ENABLE_MULTILINE_SUPPORT = "enableMultilineSupport";
  public static final Map<String, PluginPropertyField> DELIMITED_FIELDS;

  // description
  public static final String DESC_ENABLE_QUOTES =
    "Whether to treat content between quotes as a value. The default value is false.";
  public static final String DESC_SKIP_HEADER =
    "Whether to skip the first line of each file. The default value is false.";
  public static final String DESC_ENABLE_MULTILINE =
    "Whether to support content spread over multiple lines if it is between quotes. The default value is false";

  static {
    Map<String, PluginPropertyField> fields = new HashMap<>(FIELDS);
    fields.put("skipHeader", new PluginPropertyField("skipHeader", DESC_SKIP_HEADER, "boolean", false, true));
    fields.put(NAME_ENABLE_QUOTES_VALUES,
               new PluginPropertyField(NAME_ENABLE_QUOTES_VALUES, DESC_ENABLE_QUOTES, "boolean", false, true));
    fields.put(NAME_ENABLE_MULTILINE_SUPPORT,
               new PluginPropertyField(NAME_ENABLE_MULTILINE_SUPPORT, DESC_ENABLE_MULTILINE, "boolean", false, true));
    DELIMITED_FIELDS = Collections.unmodifiableMap(fields);
  }

  @Macro
  @Nullable
  @Description(DESC_ENABLE_QUOTES)
  protected Boolean enableQuotedValues;

  @Macro
  @Nullable
  @Description(DESC_ENABLE_MULTILINE)
  protected Boolean enableMultilineSupport;

  @Macro
  @Nullable
  @Description(DESC_SKIP_HEADER)
  private Boolean skipHeader;

  public DelimitedConfig() {
    super();
  }

  public boolean getSkipHeader() {
    return skipHeader != null && skipHeader;
  }

  public boolean getEnableQuotedValues() {
    return enableQuotedValues != null && enableQuotedValues;
  }

  public Boolean getEnableMultilineSupport() {
    return enableMultilineSupport != null && enableMultilineSupport;
  }

  public long getSampleSize() {
    return Long.parseLong(getProperties().getProperties().getOrDefault(NAME_SAMPLE_SIZE, "1000"));
  }

  /**
   * Parses a list of key-value items of column names and their corresponding data types, manually set by the user.
   *
   * @return A hashmap of column names and their manually set schemas.
   */
  public HashMap<String, Schema> getOverride() throws IllegalArgumentException {
    String override = getProperties().getProperties().get(NAME_OVERRIDE);
    HashMap<String, Schema> overrideDataTypes = new HashMap<>();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(override)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(override)) {
        String name = keyVal.getKey();
        String stringDataType = keyVal.getValue();

        Schema schema = null;
        switch (stringDataType) {
          case "date":
            schema = Schema.of(Schema.LogicalType.DATE);
            break;
          case "time":
            schema = Schema.of(Schema.LogicalType.TIME_MICROS);
            break;
          case "timestamp":
            schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
            break;
          default:
            schema = Schema.of(Schema.Type.valueOf(stringDataType.toUpperCase()));
        }

        if (overrideDataTypes.containsKey(name)) {
          throw new IllegalArgumentException(String.format("Cannot convert '%s' to multiple types.", name));
        }
        overrideDataTypes.put(name, schema);
      }
    }
    return overrideDataTypes;
  }
}
