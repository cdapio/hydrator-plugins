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

package io.cdap.plugin.format.delimited.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that specifies the supported data types that could get detected.
 */
public enum DataType {
  EMPTY("EMPTY", 0),
  INTEGER("INTEGER", 1),
  LONG("LONG", 2),
  DOUBLE("DOUBLE", 3),
  BOOLEAN("BOOLEAN", 4),
  TIME("TIME", 5),
  DATE("DATE", 6),
  STRING("STRING", 7);

  private static final Map<String, DataType> BY_NAME = new HashMap<>();
  private static final Map<Integer, DataType> BY_PRIORITY = new HashMap<>();

  static {
    for (DataType dataType : values()) {
      BY_NAME.put(dataType.name, dataType);
      BY_PRIORITY.put(dataType.priority, dataType);
    }
  }

  public final String name;
  public final int priority;

  DataType(String name, int priority) {
    this.name = name;
    this.priority = priority;
  }

  public static DataType valueOfName(String name) {
    return BY_NAME.get(name);
  }

  public static DataType valueOfPriority(int priority) {
    return BY_PRIORITY.get(priority);
  }
}
