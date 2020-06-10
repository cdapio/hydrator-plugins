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
 * A {@link StructuredRecord} that can be used to select the record with the min value of a given field.
 */
public class MinSelection extends NumberSelection {

  public MinSelection(String fieldName, Schema fieldSchema) {
    super(fieldName, fieldSchema);
  }

  @Override
  protected int compareInt(int val1, int val2) {
    return -Integer.compare(val1, val2);
  }

  @Override
  protected int compareLong(long val1, long val2) {
    return -Long.compare(val1, val2);
  }

  @Override
  protected int compareFloat(float val1, float val2) {
    return -Float.compare(val1, val2);
  }

  @Override
  protected int compareDouble(double val1, double val2) {
    return -Double.compare(val1, val2);
  }
}
