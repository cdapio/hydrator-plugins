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
import javax.annotation.Nullable;

/**
 * Calculates the sum of squares
 */
public class SumOfSquares implements AggregateFunction<Double, SumOfSquares> {

  private final String fieldName;
  private final Schema outputSchema;
  private double sumOfSquares;

  public SumOfSquares(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType =
        isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!AggregationUtils.isNumericType(fieldType)) {
      throw new IllegalArgumentException(String.format(
          "Cannot compute sum of squares on field %s because its type %s is not numeric",
          fieldName, fieldType));
    }
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))
        : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void initialize() {
    this.sumOfSquares = 0d;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }
    sumOfSquares += Math.pow(((Number) val).doubleValue(), 2d);
  }

  @Override
  public void mergeAggregates(SumOfSquares otherAgg) {
    if (otherAgg.getAggregate() == null) {
      return;
    }
    if (sumOfSquares == 0d) {
      sumOfSquares = otherAgg.getAggregate();
      return;
    }
    sumOfSquares += otherAgg.getAggregate();
  }

  @Nullable
  @Override
  public Double getAggregate() {
    return sumOfSquares;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
