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
import io.cdap.plugin.batch.aggregator.AggregationUtils;

/**
 * Calculates the average of a column. Does not protect against overflow.
 */
public class Avg implements AggregateFunction<Double, Avg> {
  private final String fieldName;
  private final Schema outputSchema;
  private double avg;
  private long count;

  public Avg(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!AggregationUtils.isNumericType(fieldType)) {
      throw new IllegalArgumentException(String.format(
        "Cannot compute avg on field %s because its type %s is not numeric", fieldName, fieldType));
    }

    // the avg is null only if the field value is always null
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void initialize() {
    this.avg = 0d;
    this.count = 0L;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }
    computeAvg(1L, (Number) val);
  }

  @Override
  public void mergeAggregates(Avg otherAgg) {
    computeAvg(otherAgg.count, otherAgg.avg);
  }

  @Override
  public Double getAggregate() {
    // this only happens when every value is null
    if (count == 0) {
      return null;
    }
    return avg;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }

  private void computeAvg(long deltaCount, Number oldAvg) {
    if (deltaCount == 0L) {
      return;
    }
    count += deltaCount;
    avg = avg + (oldAvg.doubleValue() - avg) * deltaCount / count;
  }
}
