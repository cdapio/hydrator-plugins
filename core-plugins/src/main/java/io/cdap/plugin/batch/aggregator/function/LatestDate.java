/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Returns the latest date in the group
 *
 * @param <T> type of aggregate value
 */
public class LatestDate<T> implements AggregateFunction<T, LatestDate<T>> {

  private final String fieldName;
  private final Schema fieldSchema;
  private T latestDate;
  private LocalDate latestDateValue;

  public LatestDate(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    LogicalType logicalType =
        fieldSchema.isNullable() ? fieldSchema.getNonNullable().getLogicalType()
            : fieldSchema.getLogicalType();
    if (!Optional.ofNullable(logicalType).isPresent() || !logicalType.equals(LogicalType.DATE)) {
      invalidType();
    }
  }

  private void invalidType() {
    throw new IllegalArgumentException(
        String.format("Field '%s' is of unsupported non-date type ",
            fieldName));
  }

  @Override
  public void initialize() {
    latestDate = null;
    latestDateValue = null;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    LocalDate value = record.getDate(fieldName);
    if (value != null) {
      if (latestDate == null || value.isAfter(latestDateValue)) {
        setLatestDate(record);
      }
    }
  }

  @Override
  public void mergeAggregates(LatestDate<T> otherAgg) {
    if (otherAgg.getAggregate() == null) {
      return;
    }
    if (latestDate == null || otherAgg.latestDateValue.isAfter(latestDateValue)) {
      latestDate = otherAgg.latestDate;
      latestDateValue = otherAgg.latestDateValue;
    }
  }

  @Override
  public T getAggregate() {
    return latestDate;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  private void setLatestDate(StructuredRecord record) {
    latestDate = record.get(fieldName);
    latestDateValue = record.getDate(fieldName);
  }
}
