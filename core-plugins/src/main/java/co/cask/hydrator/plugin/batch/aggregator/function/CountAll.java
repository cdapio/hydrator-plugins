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

/**
 * Counts the number of records in a group. This is the function for count(*).
 */
public class CountAll implements AggregateFunction<Long> {
  private long count;

  @Override
  public void beginFunction() {
    count = 0;
  }

  @Override
  public void operateOn(StructuredRecord record) {
    count++;
  }

  @Override
  public Long getAggregate() {
    return count;
  }

  @Override
  public Schema getOutputSchema() {
    return Schema.of(Schema.Type.LONG);
  }
}
