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
import co.cask.hydrator.plugin.batch.aggregator.AggrCommon;

/**
 * Calculates the Standard Deviation
 */
public class Stddev implements AggregateFunction<Double> {
  private final String fieldName;
  private final Schema outputSchema;
  private double sum1;
  private double sum2;
  private int count;
  private double stddev;

  public Stddev(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!AggrCommon.isNumericType(fieldType)) {
      throw new IllegalArgumentException(String.format(
        "Cannot compute avg on field %s because its type %s is not numeric", fieldName, fieldType));
    }
    outputSchema = isNullable ? Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)) : Schema.of(Schema.Type.DOUBLE);
  }

  @Override
  public void beginFunction() {
    sum1 = 0d;
    sum2 = 0d;
    count = 0;
    stddev = 0d;
  }

  @Override
  public void operateOn(StructuredRecord record) {
    Object val = record.get(fieldName);
    if (val == null) {
      return;
    }
    double value = ((Number) val).doubleValue();
    sum1 += value;
    sum2 += Math.pow(value, 2);
    stddev = Math.sqrt(count*sum2 - Math.pow(sum1, 2));
    count++;
  }

  @Override
  public Double getAggregate() {
    if (count == 0) {
      // only happens if the field value was always null
      return null;
    }
    return stddev;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
