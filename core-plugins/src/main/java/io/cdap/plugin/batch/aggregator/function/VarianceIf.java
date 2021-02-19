/*
 * Copyright © 2021 Cask Data, Inc.
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

import java.io.Serializable;

/**
 * Calculates Variance based on the given condition. For example condition = value > 100
 * Uses online algorithm from wikipedia to compute the variance
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
 * Uses https://www.tandfonline.com/doi/abs/10.1080/00031305.2014.966589 as the way to combine variance from two
 * splits.
 */
public class VarianceIf extends Variance implements Serializable {
  private final Condition condition;

  public VarianceIf(String fieldName, Schema fieldSchema, Condition condition) {
    super(fieldName, fieldSchema);
    this.condition = condition;
  }

  @Override
  public void mergeValue(StructuredRecord record) {
    if (!meetCondition(record, condition)) {
      return;
    }
    super.mergeValue(record);
  }
}
