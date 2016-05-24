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

import co.cask.cdap.api.data.schema.Schema;
import org.junit.Test;

/**
 */
public class MaxTest extends NumberTest {

  @Test
  public void testIntMax() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Max max = new Max("x", Schema.of(Schema.Type.INT));
    testFunction(max, schema, 101, 99, 100, 101);
    testFunction(max, schema, 100, -100, 0, 3, 100);
    testFunction(max, schema, -5, -5, -5);
  }

  @Test
  public void testLongMax() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Max max = new Max("x", Schema.of(Schema.Type.LONG));
    testFunction(max, schema, Long.MAX_VALUE, -1L, 0L, Long.MAX_VALUE, 500L);
    testFunction(max, schema, 0L, 0L);
  }

  @Test
  public void testFloatMax() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Max max = new Max("x", Schema.of(Schema.Type.FLOAT));
    testFunction(max, schema, Float.MAX_VALUE, -1.1f, 0f, Float.MAX_VALUE, 500.2f);
  }

  @Test
  public void testDoubleMax() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Max max = new Max("x", Schema.of(Schema.Type.DOUBLE));
    testFunction(max, schema, Double.MAX_VALUE, -1.1d, 0d, Double.MAX_VALUE, 500.2d);
  }
}
