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

public class VarianceIfTest extends NumberTest {

  @Test
  public void testIntCondition() {
    String condition = "x>-11";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Variance variance = new VarianceIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    Variance variance1 = new VarianceIf("x", Schema.of(Schema.Type.INT), JexlCondition.of(condition));
    testFunction(variance, schema, variance1, 0d, 0);
    testFunction(variance, schema, variance1, 2.91666666d, 1, 2, 3, 4, 5, 6);
    testFunction(variance, schema, variance1, 1986.6875d, -10, 0, 3, 100);
  }

  @Test
  public void testLongCondition() {
    String condition = "x>-150L";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Variance variance = new VarianceIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    Variance variance1 = new VarianceIf("x", Schema.of(Schema.Type.LONG), JexlCondition.of(condition));
    testFunction(variance, schema, variance1, 0d, 0L);
    testFunction(variance, schema, variance1, 2.91666666d, 1L, 2L, 3L, 4L, 5L, 6L);
    testFunction(variance, schema, variance1, 1986.6875d, -10L, 0L, 3L, 100L);
  }

  @Test
  public void testFloatCondition() {
    String condition = "x<2000.0f";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Variance variance = new VarianceIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    Variance variance1 = new VarianceIf("x", Schema.of(Schema.Type.FLOAT), JexlCondition.of(condition));
    testFunction(variance, schema, variance1, 0d, 0f);
    testFunction(variance, schema, variance1, 2.91666666d, 1f, 2f, 3f, 4f, 5f, 6f);
    testFunction(variance, schema, variance1, 1986.6875d, -10f, 0f, 3f, 100f);
    testFunction(variance, schema, variance1, 0.00175519d, 0f, 0.1f, 0.01f, 0.001f);
  }

  @Test
  public void testDoubleCondition() {
    String condition = "x>-20d";

    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Variance variance = new VarianceIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    Variance variance1 = new VarianceIf("x", Schema.of(Schema.Type.DOUBLE), JexlCondition.of(condition));
    testFunction(variance, schema, variance1, 0d, 0d);
    testFunction(variance, schema, variance1, 2.91666666d, 1d, 2d, 3d, 4d, 5d, 6d);
    testFunction(variance, schema, variance1, 1986.6875d, -10d, 0d, 3d, 100d);
    testFunction(variance, schema, variance1, 0.00175519d, 0d, 0.1d, 0.01d, 0.001d);
  }
}
