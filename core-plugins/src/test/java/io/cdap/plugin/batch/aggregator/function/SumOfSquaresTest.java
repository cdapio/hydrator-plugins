/*
 * Copyright © 2020 Cask Data, Inc.
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
public class SumOfSquaresTest extends NumberTest {

  @Test
  public void testIntVar() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    SumOfSquares sumOfSquares = new SumOfSquares("x", Schema.of(Schema.Type.INT));
    SumOfSquares sumOfSquares1 = new SumOfSquares("x", Schema.of(Schema.Type.INT));
    testFunction(sumOfSquares, schema, sumOfSquares1, 0d, 0);
    testFunction(sumOfSquares, schema, sumOfSquares1, 91d, 1, 2, 3, 4, 5, 6);
    testFunction(sumOfSquares, schema, sumOfSquares1, 10109d, -10, 0, 3, 100);
  }
}
