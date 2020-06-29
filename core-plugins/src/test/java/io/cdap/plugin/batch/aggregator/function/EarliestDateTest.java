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

import java.util.Arrays;
/**
 *
 */
public class EarliestDateTest extends AggregateFunctionTest {

  @Test
  public void earliestDateTest() {
    Schema fieldSchema = Schema.of(Schema.LogicalType.DATE);
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.nullableOf(fieldSchema)));
    test(new EarliestDate("x", fieldSchema), schema, "x", 1593387802,
        Arrays.asList(1593387802, 1593470602, 1593989002, 1595976202, 1624920202),
        new EarliestDate("x", fieldSchema));
    test(new EarliestDate("x", fieldSchema), schema, "x", 1593470602,
        Arrays.asList(null, 1593470602, 1593989002, 1595976202, 1624920202),
        new EarliestDate("x", fieldSchema));
  }

}
