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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;

public class SumIfTest extends NumberTest {

  @Test
  public void testIntCondition() {
    String condition = "x.equals(0)";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Sum sum = new SumIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    Sum sum1 = new SumIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    testFunction(sum, schema, sum1, 0, -50, 49, 1, 0, 100, -100);
    testFunction(sum, schema, sum1, 0, -100, 0, 3, 100, -3);
    testFunction(sum, schema, sum1, 0, 0);
  }

  @Test
  public void testLongCondition() {
    String condition = "x<1000L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Sum sum = new SumIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    Sum sum1 = new SumIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    testFunction(sum, schema, sum1, 500L, -1L, 0L, 1L, 500L);
    testFunction(sum, schema, sum1, 0L, 0L);
  }

  @Test
  public void testFloatCondition() {
    String condition = "x<60.50f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Sum sum = new SumIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    Sum sum1 = new SumIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    testFunction(sum, schema, sum1, 0f, -1.1f, 1.1f, 0f, -50f, 50f);
    testFunction(sum, schema, sum1, 3.14f, 0f, 3.1f, 0.04f);
  }

  @Test
  public void testDoubleCondition() {
    String condition = "x>-100.0d";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Sum sum = new SumIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    Sum sum1 = new SumIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    testFunction(sum, schema, sum1, 0d, -1.1d, 1.1d, 0d, -50d, 50d);
    testFunction(sum, schema, sum1, 3.14d, 0d, 3.1d, 0.04d);
  }
}
