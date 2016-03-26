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
public class MinTest extends NumberTest {

  @Test
  public void testIntMin() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Min min = new Min("x", Schema.of(Schema.Type.INT));
    testFunction(min, schema, 99, 99, 100, 101);
    testFunction(min, schema, -100, -100, 0, 3, 100);
    testFunction(min, schema, -5, -5, -5);
  }

  @Test
  public void testLongMin() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Min min = new Min("x", Schema.of(Schema.Type.LONG));
    testFunction(min, schema, Long.MIN_VALUE, -1L, 0L, Long.MIN_VALUE, 500L);
    testFunction(min, schema, 0L, 0L);
  }

  @Test
  public void testFloatMin() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Min min = new Min("x", Schema.of(Schema.Type.FLOAT));
    testFunction(min, schema, -1.1f, -1.1f, 0f, Float.MIN_NORMAL, 500.2f);
  }

  @Test
  public void testDoubleMin() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Min min = new Min("x", Schema.of(Schema.Type.DOUBLE));
    testFunction(min, schema, -1.1d, -1.1d, 0d, Double.MIN_NORMAL, 500.2d);
  }
}
