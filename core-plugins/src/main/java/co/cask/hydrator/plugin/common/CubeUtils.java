/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.base.Function;
import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for usage by batch and real-time cube sinks.
 */
public final class CubeUtils {

  private CubeUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static Map<String, String> parseAndGetProperties(String property, String propertyValue, String delimiter,
                                                          String fieldDelimiter, Function<String, String> keyFunction) {
    Map<String, String> result = new HashMap<>();
    List<String> fieldGroups = Arrays.asList(propertyValue.split(delimiter));
    for (String group : fieldGroups) {
      String[] split = group.split(fieldDelimiter);
      if (split.length != 2) {
        //should not happen
        throw new IllegalArgumentException(
          String.format("For Property %s Parameter %s, name or value is empty, please check config", property, group));
      }
      if (Strings.isNullOrEmpty(split[0])) {
        throw new IllegalArgumentException(
          String.format("Empty key/value is not allowed in property %s, group %s", property, group));
      }
      if (Strings.isNullOrEmpty(split[1])) {
        throw new IllegalArgumentException(
          String.format("Empty key/value is not allowed in property %s, group %s", property, group));
      }
      result.put(keyFunction.apply(split[0]), split[1]);
    }
    return result;
  }
}
