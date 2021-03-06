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

/**
 *
 */
public class AvgIfTest extends NumberTest {

  @Test
  public void testIntCondition() {
    String condition = "x>1";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    AvgIf avg = new AvgIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    AvgIf avg1 = new AvgIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    testFunction(avg, schema, avg1, null, 0);
    testFunction(avg, schema, avg1, 20d / 5d, 1, 2, 3, 4, 5, 6);
    testFunction(avg, schema, avg1, 103d / 2d, -10, 0, 3, 100);
  }

  @Test
  public void testLongCondition() {
    String condition = "x<101L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Avg avg = new AvgIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    Avg avg1 = new AvgIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    testFunction(avg, schema, avg1, 0d, 0);
    testFunction(avg, schema, avg1, 21d / 6d, 1L, 2L, 3L, 4L, 5L, 6L, 140L);
    testFunction(avg, schema, avg1, 93d / 4d, -10L, 0L, 3L, 100L, 101L);
  }

  @Test
  public void testFloatCondition() {
    String condition = "x>-100f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Avg avg = new AvgIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    Avg avg1 = new AvgIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    testFunction(avg, schema, avg1, 0d, 0);
    testFunction(avg, schema, avg1, 21d / 6d, 1f, 2f, 3f, 4f, 5f, 6f, -101F);
    testFunction(avg, schema, avg1, 93d / 4d, -10f, 0f, 3f, 100f, -101F);
    testFunction(avg, schema, avg1, 0.111d / 4d, 0f, 0.1f, 0.01f, 0.001f, -101F);
  }

  @Test
  public void testDoubleCondition() {
    String condition = "!x.equals(0)";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Avg avg = new AvgIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    Avg avg1 = new AvgIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    testFunction(avg, schema, avg1, 21d / 6d, 1d, 2d, 3d, 4d, 5d, 6d, 0);
    testFunction(avg, schema, avg1, 93d / 4d, -10d, 0d, 0, 3d, 100d);
    testFunction(avg, schema, avg1, 0.111d / 4d, 0d, 0, 0.1d, 0.01d, 0.001d);
  }

}
