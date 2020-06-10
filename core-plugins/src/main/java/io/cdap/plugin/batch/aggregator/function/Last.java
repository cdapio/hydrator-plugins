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

import javax.annotation.Nullable;

/**
 * Return the last element in the group.
 *
 * @param <T> type of aggregate value
 */
public class Last<T> implements SelectionFunction, AggregateFunction<T, Last<T>> {
  private final String fieldName;
  private final Schema fieldSchema;
  private T last;

  public Last(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
  }

  @Override
  public void initialize() {
    this.last = null;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    last = record.get(fieldName);
  }

  @Override
  public void mergeAggregates(Last<T> otherAgg) {
    last = otherAgg.getAggregate();
  }

  @Nullable
  @Override
  public T getAggregate() {
    return last;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  @Override
  public StructuredRecord select(StructuredRecord record1, StructuredRecord record2) {
    return record2;
  }
}
