/*
 * Copyright © 2021 Cask Data, Inc.
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
public class CollectListIfTest extends AggregateFunctionTest {

  @Test
  public void testIntCondition() {
    String condition = "x>1";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    test(new CollectListIf("x", schema, JexlCondition.of(condition)), schema, "x",
         ImmutableList.of(2, 3, 4), ImmutableList.of(1, 2, 3, 4), new CollectListIf("x", schema,
                                                                                    JexlCondition.of(condition)));
  }

  @Test
  public void testLongCondition() {
    String condition = "x>0L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    test(new CollectListIf("x", schema, JexlCondition.of(condition)), schema, "x",
         ImmutableList.of(1L, 2L, 3L, 4L), ImmutableList.of(0L, 1L, 2L, 3L, 4L), new CollectList("x", schema));
  }

  @Test
  public void testFloatCondition() {
    String condition = "x<5.0f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    test(new CollectListIf("x", schema, JexlCondition.of(condition)), schema, "x",
         ImmutableList.of(1.0F, 2.0F, 3.0F, 4.0F), ImmutableList.of(1.0F, 2.0F, 3.0F, 4.0F, 5.0F),
         new CollectListIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testDoubleCondition() {
    String condition = "x<5.0d";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    test(new CollectListIf("x", schema, JexlCondition.of(condition)), schema, "x",
         ImmutableList.of(1.0d, 2.0d, 3.0d, 4.0d), ImmutableList.of(1.0d, 2.0d, 3.0d, 4.0d, 5.0d),
         new CollectListIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testStringCondition() {
    String condition = "!x.equals(\"e\")";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    test(new CollectListIf("x", schema, JexlCondition.of(condition)), schema, "x",
         ImmutableList.of("a", "b", "c", "d"), ImmutableList.of("a", "b", "c", "d", "e"),
         new CollectListIf("x", schema, JexlCondition.of(condition)));
  }
}
