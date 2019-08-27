/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Util class that provides utility methods for {@link Schema}.
 */
public class Schemas {

  /**
   * Returns the display name for the provided schema. The display name is determined by the mapping of logical type
   * to display type on pipeline UI. For example, pipeline UI shows TIMESTAMP_MICROS as timestamp
   * and TIMESTAMP_MILLIS as long.
   *
   * LogicalType.DATE -> "date"
   * LogicalType.TIME_MILLIS -> "long"
   * LogicalType.TIME_MACROS -> "time"
   * LogicalType.TIMESTAMP_MILLIS -> "long"
   * LogicalType.TIMESTAMP_MACROS -> "timestamp"
   * LogicalType.DECIMAL -> "decimal"
   *
   * @param schema schema to be used to get type name
   * @return type name associated to the schema
   */
  public static String getDisplayName(Schema schema) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "date";
        case TIME_MICROS:
          return "time";
        case TIMESTAMP_MICROS:
          return "timestamp";
        case DECIMAL:
          return "decimal";
      }
    }

    return nonNullableSchema.getType().name().toLowerCase();
  }

  private Schemas() {
    // no-op
  }
}
