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
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import java.util.Set;

/**
 * @author Harsh Takkar
 */
public class CollectSetTest {

  @Test
  public void testIntCollectSet() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    CollectSet collectSet = new CollectSet("x", Schema.of(Schema.Type.INT));
    collectSet.beginFunction();
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 2).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1).build());
    Set set = collectSet.getAggregate();
    Set<Integer> expectedSet = ImmutableSet.of(1, 2);
    Assert.assertEquals(expectedSet, set);
  }

  @Test
  public void testLongCollectSet() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    CollectSet collectSet = new CollectSet("x", Schema.of(Schema.Type.LONG));
    collectSet.beginFunction();
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1L).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 2L).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1L).build());
    Set set = collectSet.getAggregate();
    Set<Long> expectedSet = ImmutableSet.of(1L, 2L);
    Assert.assertEquals(expectedSet, set);
  }

  @Test
  public void testFloatCollectSet() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    CollectSet collectSet = new CollectSet("x", Schema.of(Schema.Type.FLOAT));
    collectSet.beginFunction();
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1.0F).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 2.0F).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1.0F).build());
    Set set = collectSet.getAggregate();
    Set<Float> expectedSet = ImmutableSet.of(1.0F, 2.0F);
    Assert.assertEquals(expectedSet, set);
  }

  @Test
  public void testDoubleCollectSet() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    CollectSet collectSet = new CollectSet("x", Schema.of(Schema.Type.DOUBLE));
    collectSet.beginFunction();
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1.0D).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 2.0D).build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", 1.0D).build());
    Set set = collectSet.getAggregate();
    Set<Double> expectedSet = ImmutableSet.of(1.0D, 2.0D);
    Assert.assertEquals(expectedSet, set);
  }

  @Test
  public void testStringCollectSet() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    CollectSet collectSet = new CollectSet("x", Schema.of(Schema.Type.STRING));
    collectSet.beginFunction();
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", "a").build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", "b").build());
    collectSet.operateOn(StructuredRecord.builder(schema).set("x", "a").build());
    Set set = collectSet.getAggregate();
    Set<String> expectedSet = ImmutableSet.of("a", "b");
    Assert.assertEquals(expectedSet, set);
  }
}
