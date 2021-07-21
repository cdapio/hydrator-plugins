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

package io.cdap.plugin.common;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for plugin configuration
 */
public class ConfigUtil {
  public static final String NAME_CONNECTION = "connection";
  public static final String NAME_USE_CONNECTION = "useConnection";

  public static Map<String, String> parseKeyValueConfig(String configValue, String delimiter,
                                                        String keyValueDelimiter) {
    Map<String, String> map = new HashMap<>();
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter);
      String key = parts[0];
      String value = parts[1];
      map.put(key, value);
    }
    return map;
  }

  public static String getKVPair(String key, String value, String keyValueDelimiter) {
    return String.format("%s%s%s", key, keyValueDelimiter, value);
  }

  public static void validateConnection(PluginConfig config, Boolean useConnection, PluginConfig connection,
                                        FailureCollector collector) {
    Map<String, String> rawProperties = config.getRawProperties().getProperties();
    // if use connection is false but connection is provided, fail the validation
    if (useConnection != null && !useConnection && rawProperties.get(NAME_CONNECTION) != null) {
      collector.addFailure(
        String.format("Connection cannot be used when %s is set to false.", NAME_USE_CONNECTION),
        String.format("Please set %s to true.", NAME_USE_CONNECTION)).withConfigProperty(NAME_USE_CONNECTION);
    }
    // if use connection is true but connection is not provided , fail the validation
    if (useConnection != null && useConnection && rawProperties.get(NAME_CONNECTION) == null) {
      collector.addFailure(
        String.format(
          "Property %s is not provided when %s is set to true.",
          NAME_CONNECTION, NAME_USE_CONNECTION),
        String.format(
          "Please set a value with a Macro referencing an existing connection for property %s.", NAME_CONNECTION)
      ).withConfigProperty(NAME_CONNECTION);
    }
  }
}
