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

package io.cdap.plugin.batch.aggregator.function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

/**
 * Base class for number based selection functions.
 * Allows subclasses to implement typed methods instead of implementing their own casting logic.
 * Guarantees that only methods for one type will be called for each aggregate. For example,
 * if {@link #select(StructuredRecord, StructuredRecord)} is called,
 * only {@link #compareInt(int, int)} will be called.
 */
public abstract class NumberSelection implements SelectionFunction {
  private final String fieldName;
  private final Schema.Type fieldType;
  private final Schema.LogicalType fieldLogicalType;

  public NumberSelection(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    this.fieldLogicalType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getLogicalType() :
            fieldSchema.getLogicalType();
  }

  protected abstract int compareInt(int val1, int val2);

  protected abstract int compareLong(long val1, long val2);

  protected abstract int compareFloat(float val1, float val2);

  protected abstract int compareDouble(double val1, double val2);

  protected abstract int compareBigDecimal(BigDecimal val1, BigDecimal val2);

  protected abstract int compareLocalDateTime(LocalDateTime val1, LocalDateTime val2);

  protected abstract int compareLocalDate(LocalDate date, LocalDate date1);

  protected abstract int compareZonedDateTime(ZonedDateTime timestamp, ZonedDateTime timestamp1);

  protected abstract int compareLocalTime(LocalTime time, LocalTime time1);


  @Override
  public StructuredRecord select(StructuredRecord record1, StructuredRecord record2) {
    Object val1 = record1.get(fieldName);
    Object val2 = record2.get(fieldName);

    if (val1 == null && val2 == null) {
      return record1;
    }
    if (val1 == null) {
      return record2;
    }
    if (val2 == null) {
      return record1;
    }

    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATETIME:
          return compareLocalDateTime(record1.getDateTime(fieldName), record2.getDateTime(fieldName)) > 0 ?
                  record1 : record2;
        case DECIMAL:
          return compareBigDecimal(record1.getDecimal(fieldName), record2.getDecimal(fieldName)) > 0 ?
                  record1 : record2;
        case DATE:
          return compareLocalDate(record1.getDate(fieldName), record2.getDate(fieldName)) > 0 ?
                  record1 : record2;
        case TIMESTAMP_MILLIS:
          return compareZonedDateTime(record1.getTimestamp(fieldName), record2.getTimestamp(fieldName)) > 0 ?
                  record1 : record2;
        case TIMESTAMP_MICROS:
          return compareZonedDateTime(record1.getTimestamp(fieldName), record2.getTimestamp(fieldName)) > 0 ?
                  record1 : record2;
        case TIME_MILLIS:
          return compareLocalTime(record1.getTime(fieldName), record2.getTime(fieldName)) > 0 ?
                  record1 : record2;
        case TIME_MICROS:
          return compareLocalTime(record1.getTime(fieldName), record2.getTime(fieldName)) > 0 ?
                  record1 : record2;
        default:
          throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                  fieldName, fieldType));
      }
    } else {
        switch (fieldType) {
          case INT:
            return compareInt((Integer) val1, (Integer) val2) > 0 ? record1 : record2;
          case LONG:
            return compareLong((Long) val1, (Long) val2) > 0 ? record1 : record2;
          case FLOAT:
            return compareFloat((Float) val1, (Float) val2) > 0 ? record1 : record2;
          case DOUBLE:
            return compareDouble((Double) val1, (Double) val2) > 0 ? record1 : record2;
          default:
            throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                    fieldName, fieldType));
        }
      }
  }
}
