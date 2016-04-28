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
import org.junit.Test;

/**
 */
public class CountTest {

  @Test
  public void testCount() {
    Count count = new Count("x");
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("y", Schema.of(Schema.Type.INT)));
    count.beginFunction();
    count.operateOn(StructuredRecord.builder(schema).set("x", "abc").set("y", 5).build());
    count.operateOn(StructuredRecord.builder(schema).set("y", 3).build());
    Assert.assertEquals(1L, count.getAggregate().longValue());

    count.beginFunction();
    count.operateOn(StructuredRecord.builder(schema).set("y", 1).build());
    Assert.assertEquals(0L, count.getAggregate().longValue());
  }
}
