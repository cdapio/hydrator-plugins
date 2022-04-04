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
 * Uses E(x^2) - [E(x)]^2 formula for variance calculation
 * Uses ((n1 * E1) + (n2 * E2)) / (n1 + n2) formula when merging partition
 */
public class Variance implements AggregateFunction<Double, Variance> {
  private static final String AGG_KEY = "variance";
  private static final String AGG_SQUARE_MEAN_KEY = "squareMean";
  private static final String AGG_COUNT_KEY = "count";
  private static final String AGG_MEAN_KEY = "mean";
  private final String fieldName;
  private final Schema outputSchema;
  private Double variance;
  private double mean;
  private double squaredMean;
  private long count;

  public Variance(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    AggregationUtils.ensureNumericType(fieldSchema, fieldName, "variance");
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void initialize() {
    this.variance = null;
    this.mean = 0d;
    this.squaredMean = 0d;
    this.count = 0L;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }

    Number number = (Number) val;
    double value = number.doubleValue();
    double valueSquared = number.doubleValue() * number.doubleValue();
    count++;

    // Calculate Delta of the value vs the mean and adjust the mean
    double delta = (value / count) - (mean / count);
    mean += delta;

    // Calculate delta of the squared value vs the squared mean and adjust the squared mean
    double deltaSquared = (valueSquared / count) - (squaredMean / count);
    squaredMean += deltaSquared;

    // For a single record, variance is 0.
    if (count == 1L) {
      variance = 0d;
      return;
    }
    variance = squaredMean - (mean * mean);
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
      squaredMean = otherAgg.squaredMean;
      return;
    }

    // Get values from the aggregator
    long countLeft = count;
    long countRight = otherAgg.count;
    double meanLeft = mean;
    double meanRight = otherAgg.mean;
    double squaredMeanLeft = squaredMean;
    double squaredMeanRight = otherAgg.squaredMean;

    // Calculate new count
    count = countLeft + countRight;

    // Combine both counts and means
    // We divide at every step to reduce the possibility of catastrophic cancellation
    mean = ((meanLeft / count) * countLeft) + ((meanRight / count) * countRight);
    squaredMean = ((squaredMeanLeft / count) * countLeft) + ((squaredMeanRight / count) * countRight);

    variance = squaredMean - (mean * mean);
  }

  @Nullable
  @Override
  public Double getAggregate() {
    if (variance == null) {
      return null;
    }

    return variance;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
