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

/**
 * Calculates minimum values of a field in a group.
 */
public class Min extends NumberFunction<Min> {

  public Min(String fieldName, Schema fieldSchema) {
    super(fieldName, fieldSchema);
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    combine(record.get(fieldName));
  }

  @Override
  public void mergeAggregates(Min otherAgg) {
    combine(otherAgg.getAggregate());
  }

  private void combine(Number otherNum) {
    if (otherNum == null) {
      return;
    }

    if (number == null) {
      number = otherNum;
      return;
    }

    switch (fieldType) {
      case INT:
        number = Math.min((Integer) otherNum, (Integer) number);
        return;
      case LONG:
        number = Math.min((Long) otherNum, (Long) number);
        return;
      case FLOAT:
        number = Math.min((Float) otherNum, (Float) number);
        return;
      case DOUBLE:
        number = Math.min((Double) otherNum, (Double) number);
        return;
      default:
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported non-numeric type '%s'. ",
                                                         fieldName, fieldType));
    }
  }
}
