/*
 * Copyright © 2022 Cask Data, Inc.
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

/**
 * Return the first non-null in a group of {@link StructuredRecord}s. If all values are null, returns null.
 *
 * @param <T> type of aggregate value
 */
public class Any<T> implements SelectionFunction, AggregateFunction<T, Any<T>> {
  private final String fieldName;
  private final Schema fieldSchema;
  private T val;

  public Any(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
  }

  @Override
  public void initialize() {
    this.val = null;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    if (val == null && record.get(fieldName) != null) {
      val = record.get(fieldName);
    }
  }

  @Override
  public void mergeAggregates(Any<T> otherAgg) {
    // If this is null and the other aggregate is not null, take that value.
    if (val == null && otherAgg.getAggregate() != null) {
      val = otherAgg.getAggregate();
    }
  }

  @Override
  public T getAggregate() {
    return val;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  @Override
  public StructuredRecord select(StructuredRecord record1, StructuredRecord record2) {
    T v1 = record1.get(fieldName);
    T v2 = record2.get(fieldName);

    // If the field value for the first record is null and the field value for the second record is not null,
    // return the second record.
    if (v1 == null && v2 != null) {
      return record2;
    }

    // Return first record by default.
    return record1;
  }
}
