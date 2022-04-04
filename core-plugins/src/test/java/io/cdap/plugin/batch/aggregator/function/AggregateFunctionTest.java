/*
 * Copyright © 2020 Cask Data, Inc.
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
import org.junit.Assert;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * A class which provides common methods for aggregation function test
 */
public class AggregateFunctionTest {

  protected void test(AggregateFunction function, Schema schema,
                      String fieldName, @Nullable Object expected, List<Object> vals,
                      AggregateFunction otherFunc) {
    Assert.assertEquals(expected, getAggregate(function, schema, fieldName, vals, otherFunc));
  }

  protected Object getAggregate(AggregateFunction function, Schema schema, String fieldName, List<Object> vals,
                                AggregateFunction otherFunc) {
    function.initialize();
    for (int i = 0; i < vals.size() / 2; i++) {
      function.mergeValue(StructuredRecord.builder(schema).set(fieldName, vals.get(i)).build());
    }
    otherFunc.initialize();
    for (int i = vals.size() / 2; i < vals.size(); i++) {
      otherFunc.mergeValue(StructuredRecord.builder(schema).set(fieldName, vals.get(i)).build());
    }
    function.mergeAggregates(otherFunc);
    return function.getAggregate();
  }

  protected Object getAggregateSinglePartition(Supplier<AggregateFunction> supplier,
                                               Schema schema,
                                               String fieldName,
                                               Iterator<?> iterator) {
    // Initialize empty function
    AggregateFunction current = supplier.get();
    current.initialize();

    while (iterator.hasNext()) {
      // Initialize function for this value and merge a new value.
      current.mergeValue(StructuredRecord.builder(schema).set(fieldName, iterator.next()).build());
    }

    return current.getAggregate();
  }

  protected Object getAggregateMultiplePartitions(Supplier<AggregateFunction> supplier,
                                                  Schema schema,
                                                  String fieldName,
                                                  Iterator<?> iterator) {
    // Initialize accumulator function
    AggregateFunction accum = supplier.get();
    accum.initialize();

    // Initialize window function for the next 10 records.
    AggregateFunction window = supplier.get();
    window.initialize();

    int numRecords = 0;
    boolean mergeDirection = true;

    while (iterator.hasNext()) {
      // Initialize function for this value and merge a new value.
      window.mergeValue(StructuredRecord.builder(schema).set(fieldName, iterator.next()).build());

      // Every ten records, we merge aggregators.
      if (++numRecords % 10 == 0) {
        // Merge in alternate directions
        if (mergeDirection) {
          window.mergeAggregates(accum);
          accum = window;
        } else {
          accum.mergeAggregates(window);
        }
        mergeDirection = !mergeDirection;

        // Initialize window again
        window = supplier.get();
        window.initialize();
      }
    }

    // Merge final time
    accum.mergeAggregates(window);

    return accum.getAggregate();
  }
}
