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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aggregates and arranges all data types met while investigating a data file for automated data type detection. Has
 * functionalities to detect the data type of a single value or a single column.
 */
public class DataTypeDetectorStatusKeeper {

  private Map<String, EnumSet<DataType>> dataTypeDetectionStatus = new LinkedHashMap<>();

  /**
   * Detects the datatype of a single value.
   *
   * @param value Value whom the data type would be detected from.
   * @return The {@link DataType} of the given value.
   */
  public static DataType detectValueDataType(String value) {
    return TypeInference.getDataType(value);
  }

  /**
   * Detects the data type of a single column.
   *
   * @param dataTypes Data types met while investigating the a column for automated data type detection.
   * @return The {@link Schema} of the given column.
   */
  public static Schema detectColumnDataType(EnumSet<DataType> dataTypes) {
    DataType dataType = dataTypes.stream().max(Comparator.comparing(v -> v.priority)).orElse(DataType.STRING);
    boolean isNull = dataTypes.stream().anyMatch(v -> v.name.equalsIgnoreCase(DataType.EMPTY.name));

    switch (dataType) {
      case INTEGER:
        return getSchemaOfType(Schema.Type.INT, isNull);
      case DOUBLE:
        return getSchemaOfType(Schema.Type.DOUBLE, isNull);
      case LONG:
        return getSchemaOfType(Schema.Type.LONG, isNull);
      case BOOLEAN:
        return getSchemaOfType(Schema.Type.BOOLEAN, isNull);
      /*
       * The CSV Automated Schema Detection can identify columns with string Date/Time ("2021-12T12:34", "2021-05-19")
       * or ("10:23", "10:23:54Z") values as of Date/Time data types. But the source plugin (eg., FileSource)
       * can't parse these string values to Date and Time after. Hence, the Date and Time data types are taken as
       * String for the moment. Check Jira Plugin-637.
       */
      case DATE:
        return getSchemaOfType(Schema.Type.STRING, isNull);
      case TIME:
        return getSchemaOfType(Schema.Type.STRING, isNull);

      default:
        return getSchemaOfType(Schema.Type.STRING, isNull);
    }
  }

  /**
   * Adds a data type with the corresponding column name at the status keeper.
   *
   * @param columnName Name of the column.
   * @param dataType One of the data types met while investigating the column for data type detection.
   */
  public void addDataType(String columnName, DataType dataType) {
    if (this.dataTypeDetectionStatus.containsKey(columnName)) {
      this.dataTypeDetectionStatus.get(columnName).add(dataType);
    } else {
      EnumSet<DataType> dataTypeSet = EnumSet.noneOf(DataType.class);
      dataTypeSet.add(dataType);
      this.dataTypeDetectionStatus.put(columnName, dataTypeSet);
    }
  }

  /**
   * Returns an enum set of all data types met while investigating the given column for data type detection.
   *
   * @param columnName Name of the column.
   * @return All the data types met while investigating the given column for data type detection.
   */
  public EnumSet<DataType> getColumnDataTypes(String columnName) {
    return this.dataTypeDetectionStatus.get(columnName);
  }

  /**
   * Validates whether the {@link DataTypeDetectorStatusKeeper}'s status state is empty. If so, it throws an error.
   */
  public void validateDataTypeDetector() {
    String errorMessage = "Failed to perform automated data type detection! Check if the data file has any rows. " +
      "If one single row is present, make sure to set the property \"Skip Header\" to False.";
    if (this.dataTypeDetectionStatus.isEmpty()) {
      throw new RuntimeException(errorMessage);
    }
  }

  /**
   * Gets a nullable or non-nullable schema type.
   *
   * @param type The given schema type.
   * @param isNull True if the column investigated for data type detection has null values. Otherwise, false.
   * @return The {@link Schema.Type} determined
   */
  private static Schema getSchemaOfType(Schema.Type type, boolean isNull) {
    return !isNull ? Schema.of(type) : Schema.nullableOf(Schema.of(type));
  }

  public void setDataTypeDetectionStatus(Map<String, EnumSet<DataType>> dataTypeDetectionStatus) {
    this.dataTypeDetectionStatus = dataTypeDetectionStatus;
  }

  public Map<String, EnumSet<DataType>> getDataTypeDetectionStatus() {
    return this.dataTypeDetectionStatus;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DataTypeDetectorStatusKeeper) {
      Map<String, EnumSet<DataType>> otherStatus = ((DataTypeDetectorStatusKeeper) other).getDataTypeDetectionStatus();
      if (otherStatus.equals(this.getDataTypeDetectionStatus())) {
        return true;
      }
    }
    return false;
  }
}
