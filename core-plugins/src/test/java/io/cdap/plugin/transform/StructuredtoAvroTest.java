/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

public class StructuredtoAvroTest {

  @Test
  public void testCollectionTypes() throws Exception {
    Schema schema = Schema.recordOf("c", Schema.Field.of("c", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(schema);

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("c", new String[] { "a", "b", "c" })
      .build();
    GenericRecord result = transformer.transform(record);
    Assert.assertEquals(Arrays.asList("a", "b", "c"), result.get("c"));

    record = StructuredRecord.builder(schema)
      .set("c", Arrays.asList("a", "b", "c"))
      .build();
    result = transformer.transform(record);
    Assert.assertEquals(Arrays.asList("a", "b", "c"), result.get("c"));

    record = StructuredRecord.builder(schema)
      .set("c", new HashSet<>(Arrays.asList("a", "b", "c")))
      .build();
    result = transformer.transform(record);
    Assert.assertEquals(Arrays.asList("a", "b", "c"), result.get("c"));
  }

  @Test
  public void testStructuredToAvroConversionForNested() throws Exception {
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("innerInt", Schema.of(Schema.Type.INT)),
      Schema.Field.of("innerString", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("recordField", innerSchema));

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("intField", 5)
      .set("recordField",
           StructuredRecord.builder(innerSchema)
             .set("innerInt", 7)
             .set("innerString", "hello world")
             .build()
      )
      .build();
    StructuredToAvroTransformer structuredToAvroTransformer = new StructuredToAvroTransformer(schema);
    GenericRecord result = structuredToAvroTransformer.transform(record);
    Assert.assertEquals(5, result.get("intField"));
    GenericRecord innerRecord = (GenericRecord) result.get("recordField");
    Assert.assertEquals(7, innerRecord.get("innerInt"));
    Assert.assertEquals("hello world", innerRecord.get("innerString"));
  }

  @Test
  public void testOutputSchemaUsage() throws Exception {
    Schema outputSchema = Schema.recordOf("output",
                                          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Schema inputSchema = Schema.recordOf("input",
                                         Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("age", Schema.of(Schema.Type.INT)));
    StructuredRecord record = StructuredRecord.builder(inputSchema)
      .set("id", 123L).set("name", "ABC").set("age", 10).build();

    StructuredToAvroTransformer avroTransformer = new StructuredToAvroTransformer(outputSchema);
    GenericRecord result = avroTransformer.transform(record);
    Assert.assertEquals(123L, result.get("id"));
    Assert.assertEquals("ABC", result.get("name"));
  }

  @Test
  public void testByteArrayConversionToByteBuffer() throws Exception {
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("byteArray", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("byteBuffer", Schema.of(Schema.Type.BYTES)));
    byte[] bytes = new byte[]{ 1, 2, 3, 4 };
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("byteArray", bytes)
      .set("byteBuffer", ByteBuffer.wrap(bytes))
      .build();
    StructuredToAvroTransformer avroTransformer = new StructuredToAvroTransformer(schema);
    GenericRecord result = avroTransformer.transform(record);
    Assert.assertEquals(ByteBuffer.wrap(bytes), result.get("byteBuffer"));
    Assert.assertEquals(ByteBuffer.wrap(bytes), result.get("byteArray"));
  }

  @Test
  public void testByteArrayConversionToByteBufferForNullableField() throws Exception {
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("byteArray", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("byteBuffer", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    byte[] bytes = new byte[]{ 1, 2, 3, 4 };
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("byteArray", bytes)
      .set("byteBuffer", ByteBuffer.wrap(bytes))
      .build();
    StructuredToAvroTransformer avroTransformer = new StructuredToAvroTransformer(schema);
    GenericRecord result = avroTransformer.transform(record);
    Assert.assertEquals(ByteBuffer.wrap(bytes), result.get("byteBuffer"));
    Assert.assertEquals(ByteBuffer.wrap(bytes), result.get("byteArray"));

    // test nulls
    record = StructuredRecord.builder(schema).build();
    result = avroTransformer.transform(record);
    Assert.assertNull(result.get("byteBuffer"));
    Assert.assertNull(result.get("byteArray"));
  }
}
