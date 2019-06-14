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
import java.util.List;

/**
 * @author Harsh Takkar
 */
public class CollectListTest {

  @Test
  public void testIntCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    CollectList collectList = new CollectList("x", Schema.of(Schema.Type.INT));
    collectList.beginFunction();
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 1).build());
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 2).build());
    List list = collectList.getAggregate();
    Assert.assertArrayEquals(new Integer[] {1, 2}, list.toArray());
  }

  @Test
  public void testLongCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    CollectList collectList = new CollectList("x", Schema.of(Schema.Type.LONG));
    collectList.beginFunction();
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 1L).build());
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 2L).build());
    List list = collectList.getAggregate();
    Assert.assertArrayEquals(new Long[] {1L, 2L}, list.toArray());
  }

  @Test
  public void testFloatCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    CollectList collectList = new CollectList("x", Schema.of(Schema.Type.FLOAT));
    collectList.beginFunction();
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 1.0F).build());
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 2.0F).build());
    List list = collectList.getAggregate();
    Assert.assertArrayEquals(new Float[] {1.0F, 2.0F}, list.toArray());
  }

  @Test
  public void testDoubleCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    CollectList collectList = new CollectList("x", Schema.of(Schema.Type.DOUBLE));
    collectList.beginFunction();
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 1.0D).build());
    collectList.operateOn(StructuredRecord.builder(schema).set("x", 2.0D).build());
    List list = collectList.getAggregate();
    Assert.assertArrayEquals(new Double[] {1.0D, 2.0D}, list.toArray());
  }

  @Test
  public void testStringCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    CollectList collectList = new CollectList("x", Schema.of(Schema.Type.STRING));
    collectList.beginFunction();
    collectList.operateOn(StructuredRecord.builder(schema).set("x", "a").build());
    collectList.operateOn(StructuredRecord.builder(schema).set("x", "b").build());
    List list = collectList.getAggregate();
    Assert.assertArrayEquals(new String[] {"a", "b"}, list.toArray());
  }
}
