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

import java.util.Arrays;

/**
 *
 */
public class AnyIfTest extends AggregateFunctionTest {

  @Test
  public void testFirst() {
    String condition = "x>=1";

    Schema fieldSchema = Schema.of(Schema.Type.INT);
    Schema schema = Schema.recordOf("test", Schema.Field.of("x",  Schema.nullableOf(fieldSchema)));
    test(new AnyIf("x", fieldSchema, JexlCondition.of(condition)), schema, "x", 1,
         Arrays.asList(1, 2, 3, 4, 5), new First("x", fieldSchema));
    test(new AnyIf("x", fieldSchema, JexlCondition.of(condition)), schema, "x", 5,
         Arrays.asList(5, 4, 3, 2, 1), new First("x", fieldSchema));
  }
}
