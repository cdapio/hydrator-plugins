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

package io.cdap.plugin.spark.test;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.spark.SCD2;
import io.cdap.plugin.spark.SCD2Key;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SCD2IteratorTest {

  @Test
  public void testIterator() throws Exception {
    Schema schema = Schema.recordOf(
      "x",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("other", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("startDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("endDate", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("id", 0)
                .set("other", 10).set("startDate", 0L).set("endDate", 10000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 0)
                .set("other", 10).set("startDate", 100000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 0)
                .set("startDate", 1000000000L).set("endDate", 5000000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1)
                .set("other", 1).set("startDate", 10000000L).set("endDate", 20000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1)
                .set("other", 2).set("startDate", 15000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1)
                .set("startDate", 21000000L).set("endDate", 1000000000L).build());
    List<Tuple2<SCD2Key, StructuredRecord>> inputs = input.stream().map(
      record ->
        new Tuple2<>(new SCD2Key(record.get("id"), record.get("startDate")), record)).collect(Collectors.toList());

    Iterator<StructuredRecord> iterator =
      new SCD2.SCD2Iterator(inputs.iterator(),
                            new SCD2.Conf("id", "startDate", "endDate", false, false, null, false, null));

    List<StructuredRecord> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);

    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("other", 10).set("startDate", 0L).set("endDate", 99000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("other", 10).set("startDate", 100000000L).set("endDate", 999000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("startDate", 1000000000L).set("endDate", 253402214400000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("other", 1).set("startDate", 10000000L).set("endDate", 14000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("other", 2).set("startDate", 15000000L).set("endDate", 20000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("startDate", 21000000L).set("endDate", 253402214400000000L).build());
    Assert.assertEquals(result, expected);

    // test fill in null and deduplicate
    iterator =
      new SCD2.SCD2Iterator(inputs.iterator(),
                            new SCD2.Conf("id", "startDate", "endDate", true,
                                          true, "id,startDate,endDate", false, null));

    result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    expected = new ArrayList<>();

    expected.add(null);
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("other", 10).set("startDate", 100000000L).set("endDate", 999000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("other", 10).set("startDate", 1000000000L).set("endDate", 253402214400000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("other", 1).set("startDate", 10000000L).set("endDate", 14000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("other", 2).set("startDate", 15000000L).set("endDate", 20000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("other", 2).set("startDate", 21000000L).set("endDate", 253402214400000000L).build());
    Assert.assertEquals(result, expected);
  }
}
