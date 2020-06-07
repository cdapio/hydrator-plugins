/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;

/**
 *
 */
public class CollectListTest extends AggregateFunctionTest {

  @Test
  public void testIntCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    test(new CollectList("x", schema), schema, "x",
         ImmutableList.of(1, 2, 3, 4), ImmutableList.of(1, 2, 3, 4), new CollectList("x", schema));
  }

  @Test
  public void testLongCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    test(new CollectList("x", schema), schema, "x",
         ImmutableList.of(1L, 2L, 3L, 4L), ImmutableList.of(1L, 2L, 3L, 4L), new CollectList("x", schema));
  }

  @Test
  public void testFloatCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    test(new CollectList("x", schema), schema, "x",
         ImmutableList.of(1.0F, 2.0F, 3.0F, 4.0F), ImmutableList.of(1.0F, 2.0F, 3.0F, 4.0F),
         new CollectList("x", schema));
  }

  @Test
  public void testDoubleCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    test(new CollectList("x", schema), schema, "x",
         ImmutableList.of(1.0d, 2.0d, 3.0d, 4.0d), ImmutableList.of(1.0d, 2.0d, 3.0d, 4.0d),
         new CollectList("x", schema));
  }

  @Test
  public void testStringCollectList() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    test(new CollectList("x", schema), schema, "x",
         ImmutableList.of("a", "b", "c", "d"), ImmutableList.of("a", "b", "c", "d"), new CollectList("x", schema));
  }
}
