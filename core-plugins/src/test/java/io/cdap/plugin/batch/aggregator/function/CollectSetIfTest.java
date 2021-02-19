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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;

/**
 *
 */
public class CollectSetIfTest extends AggregateFunctionTest {

  @Test
  public void testIntCondition() {
    String condition = "x>1";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    test(new CollectSetIf("x", schema, JexlCondition.of(condition)), schema, "x", ImmutableSet.of(2),
         ImmutableList.of(1, 2, 2, 1), new CollectSetIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testLongCondition() {
    String condition = "x>0L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    test(new CollectSetIf("x", schema, JexlCondition.of(condition)), schema, "x", ImmutableSet.of(1L, 2L),
         ImmutableList.of(1L, 2L, 1L, -1L), new CollectSetIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testFloatCondition() {
    String condition = "x<3.0f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    test(new CollectSetIf("x", schema, JexlCondition.of(condition)), schema, "x", ImmutableSet.of(1.0F, 2.0F),
         ImmutableList.of(1.0F, 2.0F, 1.0F, 3.0F, 4.0F), new CollectSetIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testDoubleCondition() {
    String condition = "x>0d";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    test(new CollectSetIf("x", schema, JexlCondition.of(condition)), schema, "x", ImmutableSet.of(1.0D, 2.0D),
         ImmutableList.of(1.0D, 2.0D, 1.0D, -1D), new CollectSetIf("x", schema, JexlCondition.of(condition)));
  }

  @Test
  public void testStringCondition() {
    String condition = "!x.equals(\"3\")";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    test(new CollectSetIf("x", schema, JexlCondition.of(condition)), schema, "x", ImmutableSet.of("1", "2"),
         ImmutableList.of("1", "2", "1", "3"), new CollectSetIf("x", schema, JexlCondition.of(condition)));
  }
}
