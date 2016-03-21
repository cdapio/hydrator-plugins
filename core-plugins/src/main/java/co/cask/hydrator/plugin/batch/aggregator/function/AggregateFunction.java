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
 * Performs an aggregation. For each group that needs an aggregate to be calculated, the {@link #beginAggregate()}
 * method is called first. After that, one or more calls to {@link #update(StructuredRecord)} are made, one call for
 * each value in the group. Finally, {@link #finishAggregate()} is called to retrieve the final aggregate value.
 *
 * todo: convert this to a plugin
 *
 * @param <T> type of aggregate value
 */
public interface AggregateFunction<T> {

  /**
   * Called once to start calculation of an aggregate.
   */
  void beginAggregate();

  /**
   * Called once for each record to aggregate. Calls to update are made after the call to {@link #beginAggregate()}.
   *
   * @param record the record to aggregate
   */
  void update(StructuredRecord record);

  /**
   * Called once when an aggregate should be fetched. Called after all calls to {@link #update(StructuredRecord)}
   * for an aggregate have been made.
   *
   * @return the aggregate value
   */
  T finishAggregate();

  /**
   * @return the schema of the aggregate values returned by this function.
   */
  Schema getOutputSchema();
}
