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
import org.junit.Assert;

/**
 */
public class NumberTest {
  
  protected void testFunction(AggregateFunction func, Schema schema, Number expected, Number... inputs) {
    func.beginFunction();
    for (Number num : inputs) {
      func.operateOn(StructuredRecord.builder(schema).set("x", num).build());
    }
    if (expected instanceof Float) {
      Assert.assertTrue(Math.abs((float) expected - (float) func.getAggregate()) < 0.000001f);
    } else if (expected instanceof Double) {
      Assert.assertTrue(Math.abs((double) expected - (double) func.getAggregate()) < 0.000001d);
    } else {
      Assert.assertEquals(expected, func.getAggregate());
    }
  }
}
