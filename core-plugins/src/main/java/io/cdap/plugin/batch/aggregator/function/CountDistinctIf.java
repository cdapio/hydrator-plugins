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

import java.io.Serializable;

/**
 * Count distinct values of a specific column based on the given condition. For example
 * condition = value.equals("Example")
 *
 * @param <T> type of aggregate value
 */
public class CountDistinctIf<T> extends CountDistinct<T> implements Serializable {
  private final Condition condition;

  public CountDistinctIf(String fieldName, Condition condition) {
    super(fieldName);
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
