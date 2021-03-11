/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Common functions for aggregation related functionalities.
 */
public final class AggregationUtils {

  private AggregationUtils() {
  }

  /**
   * Checks if the type of the field is Numeric.
   *
   * @param fieldType type of the field to be checked.
   * @return true if numeric, false otherwise.
   */
  private static boolean isNumericType(Schema.Type fieldType) {
    return fieldType == Schema.Type.INT || fieldType == Schema.Type.LONG ||
      fieldType == Schema.Type.FLOAT || fieldType == Schema.Type.DOUBLE;
  }

  /**
   * Validates if the field schema type is numeric, otherwise throws an IllegalArgumentException
   *
   * @param fieldSchema  - Schema for the field
   * @param fieldName    - Name of the field
   * @param functionName - Name of the function
   */
  public static void ensureNumericType(Schema fieldSchema, String fieldName, String functionName) {
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!isNumericType(fieldType)) {
      generateException(fieldSchema, fieldName, functionName, fieldType, "numeric");
    }
  }

  /**
   * Validates if the field schema type is boolean, otherwise throws an IllegalArgumentException
   *
   * @param fieldSchema  - Schema for the field
   * @param fieldName    - Name of the field
   * @param functionName - Name of the function
   */
  public static void ensureBooleanType(Schema fieldSchema, String fieldName, String functionName) {
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (fieldType != Schema.Type.BOOLEAN) {
      generateException(fieldSchema, fieldName, functionName, fieldType, "boolean");
    }
  }

  private static void generateException(Schema fieldSchema, String fieldName, String functionName,
                                        Schema.Type fieldType, String expectedType) {
    Schema.LogicalType logicalType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getLogicalType() :
      fieldSchema.getLogicalType();
    throw new IllegalArgumentException(String.format(
      "Cannot compute %s on field %s because its type %s is not %s", functionName, fieldName,
      logicalType == null ? fieldType : logicalType, expectedType));
  }
}
