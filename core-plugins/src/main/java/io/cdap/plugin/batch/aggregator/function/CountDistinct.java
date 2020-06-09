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

import java.util.HashSet;
import java.util.Set;

/**
 * Count distinct values of a specific column
 *
 * @param <T> type of aggregate value
 */
public class CountDistinct<T> implements AggregateFunction<Integer, CountDistinct<T>> {
  private static final Schema SCHEMA = Schema.of(Schema.Type.INT);
  private final String fieldName;
  private Set<T> collectSet;

  public CountDistinct(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public void initialize() {
    collectSet = new HashSet<>();
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    collectSet.add(record.get(fieldName));
  }

  @Override
  public void mergeAggregates(CountDistinct<T> otherAgg) {
    collectSet.addAll(otherAgg.collectSet);
  }

  @Override
  public Integer getAggregate() {
    return collectSet.size();
  }

  @Override
  public Schema getOutputSchema() {
    return SCHEMA;
  }
}
