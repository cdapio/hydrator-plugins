/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import java.util.Arrays;

/**
 *
 */
public class CountTest extends AggregateFunctionTest {

  @Test
  public void testCount() {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    test(new Count("x"), schema, "x", 3L, Arrays.asList(1, 2, null, null, 3), new Count("x"));
    test(new Count("y"), schema, "x", 0L, Arrays.asList(1, 2, null, null, 3), new Count("y"));
  }
}
