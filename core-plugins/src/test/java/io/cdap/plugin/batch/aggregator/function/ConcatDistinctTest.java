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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * A test method for ConcatDistinct aggregate function
 */
public class ConcatDistinctTest extends AggregateFunctionTest {
  static final Schema FIELD_SCHEMA = Schema.of(Schema.Type.STRING);
  static final Schema RECORD_SCHEMA =
    Schema.recordOf("test", Schema.Field.of("x", Schema.nullableOf(FIELD_SCHEMA)));

  @Test
  public void testConcatDistinct() {
    Schema schema = Schema.recordOf("test", Schema.Field.of("x", Schema.nullableOf(FIELD_SCHEMA)));
    test(new ConcatDistinct("x", FIELD_SCHEMA), schema, "x", "1, 2, 3, 4, 5",
         Arrays.asList("1", "2", "3", "4", "5"), new ConcatDistinct("x", FIELD_SCHEMA));
    test(new ConcatDistinct("x", FIELD_SCHEMA), schema, "x", "2, 3, 4, 5",
         Arrays.asList(null, "2", "3", "4", "5"), new ConcatDistinct("x", FIELD_SCHEMA));
    test(new ConcatDistinct("x", FIELD_SCHEMA), schema, "x", "2, 3, 5",
         Arrays.asList(null, "2", "2", "2", "3", "3", "5"), new ConcatDistinct("x", FIELD_SCHEMA));
  }

  @Test
  public void testConcatMergeBothEmpty() {
    ConcatDistinct left = new ConcatDistinct("x", FIELD_SCHEMA);
    left.initialize();
    ConcatDistinct right = new ConcatDistinct("x", FIELD_SCHEMA);
    right.initialize();

    // Merge
    left.mergeAggregates(right);
    Assert.assertEquals("", left.getAggregate());
  }

  @Test
  public void testConcatMergeLeftEmpty() {
    ConcatDistinct left = new ConcatDistinct("x", FIELD_SCHEMA);
    left.initialize();
    ConcatDistinct right = new ConcatDistinct("x", FIELD_SCHEMA);
    right.initialize();

    // Populate values on the right side
    right.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "a").build());
    right.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "b").build());

    // Merge
    left.mergeAggregates(right);
    Assert.assertEquals("a, b", left.getAggregate());
  }

  @Test
  public void testConcatMergeRightEmpty() {
    ConcatDistinct left = new ConcatDistinct("x", FIELD_SCHEMA);
    left.initialize();
    ConcatDistinct right = new ConcatDistinct("x", FIELD_SCHEMA);
    right.initialize();

    // Populate values on the left side
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "a").build());
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "b").build());

    // Merge
    left.mergeAggregates(right);
    Assert.assertEquals("a, b", left.getAggregate());
  }

  @Test
  public void testConcatMerge() {
    ConcatDistinct left = new ConcatDistinct("x", FIELD_SCHEMA);
    left.initialize();
    ConcatDistinct right = new ConcatDistinct("x", FIELD_SCHEMA);
    right.initialize();

    // Populate values on the left side
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "a").build());
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "b").build());

    // Populate values on the right side
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "a").build());
    left.mergeValue(StructuredRecord.builder(RECORD_SCHEMA).set("x", "c").build());

    // Merge
    left.mergeAggregates(right);
    Assert.assertEquals("a, b, c", left.getAggregate());
  }

}
