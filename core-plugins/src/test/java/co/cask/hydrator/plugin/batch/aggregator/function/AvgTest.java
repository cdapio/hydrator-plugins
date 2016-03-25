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
public class AvgTest extends NumberTest {

  @Test
  public void testIntAvg() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Avg avg = new Avg("x", Schema.of(Schema.Type.INT));
    testFunction(avg, schema, 0d, 0);
    testFunction(avg, schema, 21d / 6d, 1, 2, 3, 4, 5, 6);
    testFunction(avg, schema, 93d / 4d, -10, 0, 3, 100);
  }

  @Test
  public void testLongAvg() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Avg avg = new Avg("x", Schema.of(Schema.Type.LONG));
    testFunction(avg, schema, 0d, 0);
    testFunction(avg, schema, 21d / 6d, 1L, 2L, 3L, 4L, 5L, 6L);
    testFunction(avg, schema, 93d / 4d, -10L, 0L, 3L, 100L);
  }

  @Test
  public void testFloatAvg() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.FLOAT)));
    Avg avg = new Avg("x", Schema.of(Schema.Type.FLOAT));
    testFunction(avg, schema, 0d, 0);
    testFunction(avg, schema, 21d / 6d, 1f, 2f, 3f, 4f, 5f, 6f);
    testFunction(avg, schema, 93d / 4d, -10f, 0f, 3f, 100f);
    testFunction(avg, schema, 0.111d / 4d, 0f, 0.1f, 0.01f, 0.001f);
  }

  @Test
  public void testDoubleAvg() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    Avg avg = new Avg("x", Schema.of(Schema.Type.DOUBLE));
    testFunction(avg, schema, 21d / 6d, 1d, 2d, 3d, 4d, 5d, 6d);
    testFunction(avg, schema, 93d / 4d, -10d, 0d, 3d, 100d);
    testFunction(avg, schema, 0.111d / 4d, 0d, 0.1d, 0.01d, 0.001d);
  }
}
