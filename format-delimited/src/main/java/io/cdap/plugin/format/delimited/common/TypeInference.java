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

import java.util.regex.Pattern;

/**
 * Type Interface provides utility functions that allow you to detect the types of data.
 */
public class TypeInference {
  private static final Pattern patternInteger = Pattern.compile("^(\\+|-)?\\d+$");

  private static final Pattern patternDouble = Pattern.compile(
    "^[-+]?"// Positive/Negative sign
      + "("// BEGIN Decimal part
      + "[0-9]+([,\\.][0-9]+)?|"// Alternative I (w/o grouped integer part)
      + "(" // BEGIN Alternative II (with grouped integer part)
      + "[0-9]{1,3}" // starting digits
      + "(" // BEGIN grouped part
      + "((,[0-9]{3})*"// US integer part
      + "(\\.[0-9]+)?"// US float part
      + "|" // OR
      + "((\\.[0-9]{3})*|([ \u00A0\u2007\u202F][0-9]{3})*)"// EU integer part
      + "(,[0-9]+)?)"// EU float part
      + ")"// END grouped part
      + ")" // END Alternative II
      + ")" // END Decimal part
      + "([ ]?[eE][-+]?[0-9]+)?$"); // scientific part


  private static final Pattern patternLong = Pattern.compile("[-+]?[0-9]+L");

  /**
   * Detects if the given value is of a double type.
   *
   * @param value The given raw string value.
   * @return True if the value is a double type, false otherwise.
   */
  public static boolean isDouble(String value) {
    if (!isEmpty(value) && patternDouble.matcher(value).matches()) {
      return true;
    }
    return false;
  }

  /**
   * Detects if the given value is of a long type.
   *
   * @param value The given raw string value.
   * @return Result whether the given value is boolean or not.
   */
  public static boolean isLong(String value) {
    if (!isEmpty(value) && patternLong.matcher(value).matches()) {
      return true;
    }
    return false;
  }

  /**
   * Detects if the given value is of a integer type.
   *
   * @param value The given raw string value.
   * @return true if the value is a integer type, false otherwise.
   */
  public static boolean isInteger(String value) {
    if (!isEmpty(value) && patternInteger.matcher(value).matches()) {
      return true;
    }
    return false;
  }

  /**
   * Detects if the given value is of a boolean type.
   *
   * @param value The given raw string value.
   * @return true if the value is a boolean type, false otherwise.
   */
  public static boolean isBoolean(String value) {
    if (isEmpty(value)) {
      return false;
    }
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  /**
   * Detects if the given value is of a date type. <br>
   *
   * @param value The given raw string value.
   * @return true if the value is a date type, false otherwise.
   */
  public static boolean isDate(String value) {
    return DateTimePattern.isDate(value);
  }

  /**
   * Detects if the given value is of a time type.
   *
   * @param value The given raw string value.
   * @return true if the value is of Time data type
   */
  public static boolean isTime(String value) {
    return DateTimePattern.isTime(value);
  }

  /**
   * Detects if the given value is blank or null.
   *
   * @param value The given raw string value.
   * @return true if the value is blank or null, false otherwise.
   */
  public static boolean isEmpty(String value) {
    return value == null || value.trim().length() == 0;
  }

  /**
   * Investigates whether the detected integer (math) number is of int, long or String data type depending on the
   * size of the number.
   *
   * @param value The given raw string value.
   * @return The data type detected. Could be one of int, long or string.
   */
  private static DataType deepInvestigateInt(String value) {
    try {
      try {
        int parsedIntValue = Integer.parseInt(value);
        return DataType.INTEGER;
      } catch (NumberFormatException e) {
        Long.parseLong(value);
        return DataType.LONG;
      }
    } catch (NumberFormatException e) {
      return DataType.STRING;
    }
  }

  /**
   * Investigates whether the detected double (math) number is of double or string data type depending on the size of
   * the number.
   *
   * @param value The given raw string value.
   * @return The data type detected. Could be one of long or string.
   */
  private static DataType deepInvestigateDouble(String value) {
    try {
      Double doubleValue = Double.parseDouble(value);
      if (doubleValue.toString().equals(value)) {
        return DataType.DOUBLE;
      }
      return DataType.STRING;
    } catch (NumberFormatException e) {
      return DataType.STRING;
    }
  }

  /**
   * Investigates whether the detected integer (math) number is of long or string data type depending on the size of
   * the number.
   *
   * @param value The given raw string value.
   * @return The data type detected. Could be one of long or string.
   */
  private static DataType deepInvestigateLong(String value) {
    try {
      Long.parseLong(value.replace("L", ""));
      return DataType.LONG;
    } catch (NumberFormatException e) {
      return DataType.STRING;
    }
  }

  /**
   * Returns the detected data type of the given value.
   *
   * @param value The given raw string value.
   * @return The inferred {@link DataType} of the given value.
   */
  public static DataType getDataType(String value) {
    if (TypeInference.isEmpty(value)) {
      return DataType.EMPTY;
    } else if (TypeInference.isBoolean(value)) {
      return DataType.BOOLEAN;
    } else if (TypeInference.isInteger(value)) {
      return deepInvestigateInt(value);
    } else if (TypeInference.isLong(value)) {
      return deepInvestigateLong(value);
    } else if (TypeInference.isDouble(value)) {
      return deepInvestigateDouble(value);
    } else if (isTime(value)) {
      return DataType.TIME;
    } else if (isDate(value)) {
      return DataType.DATE;
    }
    return DataType.STRING;
  }
}
