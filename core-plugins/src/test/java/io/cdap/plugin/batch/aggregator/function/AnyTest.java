/*
 * Copyright © 2022 Cask Data, Inc.
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
 * Test for the {@link Any} function.
 */
public class AnyTest extends AggregateFunctionTest {

  @Test
  public void testAny() {
    Schema fieldSchema = Schema.of(Schema.Type.INT);
    Schema schema = Schema.recordOf("test", Schema.Field.of("x",  Schema.nullableOf(fieldSchema)));
    test(new Any("x", fieldSchema), schema, "x", 1,
         Arrays.asList(1, 2, 3, 4, 5), new Any("x", fieldSchema));
    test(new Any("x", fieldSchema), schema, "x", 2,
         Arrays.asList(null, 2, 3, 4, 5), new Any("x", fieldSchema));
    test(new Any("x", fieldSchema), schema, "x", 3,
         Arrays.asList(null, null, 3, null, 5), new Any("x", fieldSchema));
    test(new Any("x", fieldSchema), schema, "x", 5,
         Arrays.asList(null, null, null, null, 5), new Any("x", fieldSchema));
    test(new Any("x", fieldSchema), schema, "x", null,
         Arrays.asList(null, null, null, null, null), new Any("x", fieldSchema));
  }
}
