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
 * Returns true if all the values in the group fulfill the condition given, false even if there is a single value that
 * does not fulfill the condition. For example condition = value > 1
 */
public class LogicalAndIf extends LogicalAnd implements Serializable {
  private final Condition condition;

  public LogicalAndIf(String fieldName, Schema fieldSchema, Condition condition) {
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
