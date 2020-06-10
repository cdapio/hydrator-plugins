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

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Performs an aggregation.
 *
 * @param <T> type of aggregate value
 * @param <V> type of aggregate function
 */
public interface AggregateFunction<T, V extends AggregateFunction> extends Serializable {

  /**
   * Initialize the function. This function is guaranteed to be called before any other method is called.
   */
  void initialize();

  /**
   * Merge the given record to the aggregated collection. This function is guaranteed to get called once
   *
   * @param record the record to merge
   */
  void mergeValue(StructuredRecord record);

  /**
   * Merge the given two aggregates into one aggregate, the result is stored in first aggregate.
   *
   * @param otherAgg other aggregation function to merge
   */
  void mergeAggregates(V otherAgg);

  /**
   * Get the aggregated result from the given aggregated records. This method is guaranteed to be called
   * after all the {@link #mergeValue(StructuredRecord)} and {@link #mergeAggregates(AggregateFunction)} calls
   * are done.
   *
   * @return the aggregate value
   */
  @Nullable
  T getAggregate();

  /**
   * @return the schema of the aggregate value returned by this function.
   */
  Schema getOutputSchema();
}
