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

import co.cask.cdap.api.data.schema.Schema;

/**
 * Converts a type from String to it's specified record type.
 */
public final class TypeConvertor {

  public static Object get(String value, Schema.Type type) {
    Object object = null;

    switch(type) {
      case NULL:
        object = null;
        break;

      case STRING:
        object = value;
        break;

      case INT:
        object = getInt(value);
        break;

      case LONG:
        object = getLong(value);
        break;

      case DOUBLE:
        object = getDouble(value);
        break;

      case FLOAT:
        object = getFloat(value);
        break;

      case BOOLEAN:
        object = getBoolean(value);
        break;

      case BYTES:
        object = value.getBytes();
        break;

      case ENUM:
      case ARRAY:
      case MAP:
      case RECORD:
      case UNION:
        break;

    }
    return object;

  }

  private static int getInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to INT");
    }
  }

  private static long getLong(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to LONG");
    }
  }

  private static double getDouble(String value) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to DOUBLE");
    }
  }

  private static float getFloat(String value) {
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to FLOAT");
    }
  }

  private static boolean getBoolean(String value) {
    try {
      return Boolean.parseBoolean(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to BOOLEAN");
    }
  }

}
