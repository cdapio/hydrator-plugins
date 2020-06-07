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
 * Calculates Variance
 * Uses online algorithm from wikipedia to compute the variance
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
 * Uses https://www.tandfonline.com/doi/abs/10.1080/00031305.2014.966589 as the way to combine variance from two
 * splits.
 */
public class Variance implements AggregateFunction<Double, Variance> {
  private static final String AGG_KEY = "variance";
  private static final String AGG_SQUARE_MEAN_KEY = "squareMean";
  private static final String AGG_COUNT_KEY = "count";
  private static final String AGG_MEAN_KEY = "mean";
  private final String fieldName;
  private final Schema outputSchema;
  private Double variance;
  private double squareMean;
  private double mean;
  private long count;

  public Variance(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!AggregationUtils.isNumericType(fieldType)) {
      throw new IllegalArgumentException(String.format(
        "Cannot compute variance on field %s because its type %s is not numeric", fieldName, fieldType));
    }
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void initialize() {
    this.variance = null;
    this.squareMean = 0d;
    this.mean = 0d;
    this.count = 0L;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }

    Number value = (Number) val;
    count++;

    double delta = value.doubleValue() - mean;
    mean += delta / count;
    double delta2 = value.doubleValue() - mean;
    squareMean += delta * delta2;
    // if we divide by count, the result will be population variance,
    // count - 1, we will get sample variance
    if (count == 1L) {
      variance = 0d;
      return;
    }
    variance = squareMean / (count - 1);
  }

  @Override
  public void mergeAggregates(Variance otherAgg) {
    if (otherAgg.variance == null) {
      return;
    }
    if (variance == null) {
      variance = otherAgg.variance;
      count = otherAgg.count;
      mean = otherAgg.mean;
      return;
    }

    long c1 = count;
    long c2 = otherAgg.count;
    double m1 = mean;
    double m2 = otherAgg.mean;

    count = c1 + c2;
    variance = 1d / (count - 1d) * ((c1 - 1d) * variance + (c2 - 1d) * otherAgg.variance +
                                      c1 * c2 * Math.pow(m1 - m2, 2d) / count);
    mean = (c1 * m1 + c2 * m2) / count;
  }

  @Nullable
  @Override
  public Double getAggregate() {
    if (variance != null) {
      return variance / count * (count - 1);
    }
    return variance;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
