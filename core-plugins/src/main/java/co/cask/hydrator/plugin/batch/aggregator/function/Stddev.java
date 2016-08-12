/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator.function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.aggregator.AggregationUtils;

/**
 * Calculates the Standard Deviation
 */
public class Stddev implements AggregateFunction<Double> {
  private final String fieldName;
  private final Schema outputSchema;
  private RunningStats stats;

  public Stddev(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!AggregationUtils.isNumericType(fieldType)) {
      throw new IllegalArgumentException(String.format(
        "Cannot compute standard deviation on field %s because its type %s is not numeric", fieldName, fieldType));
    }
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void beginFunction() {
    stats = new RunningStats();
  }

  @Override
  public void operateOn(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }
    double value = ((Number) val).doubleValue();
    stats.push(value);
  }

  @Override
  public Double getAggregate() {
    return stats.stddev();
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
