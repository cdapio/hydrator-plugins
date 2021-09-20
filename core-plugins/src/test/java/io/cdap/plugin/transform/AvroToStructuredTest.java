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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class AvroToStructuredTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNullValueForNonNullableField() throws IOException {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    org.apache.avro.Schema avroSchema = convertSchema(
      Schema.recordOf("nullable", Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.INT)))));
    GenericRecord avroRecord = new GenericRecordBuilder(avroSchema)
      .set("x", null)
      .build();

    Schema schema = Schema.recordOf("x", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    avroToStructuredTransformer.transform(avroRecord, schema);
  }

  @Test
  public void testAvroToStructuredConversionForNested() throws Exception {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    Schema innerNestedSchema = Schema.recordOf("innerNested",
                                               Schema.Field.of("int", Schema.of(Schema.Type.INT)));
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("array1", Schema.arrayOf(innerNestedSchema)),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));


    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);
    org.apache.avro.Schema avroInnerNestedSchema = convertSchema(innerNestedSchema);


    GenericRecord inner = new GenericRecordBuilder(avroInnerNestedSchema)
      .set("int", 0)
      .build();
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("int", 5)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .set("array1", ImmutableList.of(inner, inner))
             .set("map", ImmutableMap.of("key", "value"))
             .build())
      .build();

    StructuredRecord result = avroToStructuredTransformer.transform(record);
    Assert.assertEquals(Integer.MAX_VALUE, result.<Integer>get("int").intValue());
    StructuredRecord innerResult = result.get("record");
    Assert.assertEquals(5, innerResult.<Integer>get("int").intValue());
    Assert.assertEquals(ImmutableList.of(1.0f, 2.0f), innerResult.get("array"));
    List list = innerResult.get("array1");
    StructuredRecord array1Result = (StructuredRecord) list.get(0);
    Assert.assertEquals(0, array1Result.<Integer>get("int").intValue());
  }

  @Test
  public void testAvroToStructuredConversionForMaps() throws IOException {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    String jsonSchema = "{\"type\":\"record\", \"name\":\"rec\"," +
        "\"fields\":[{\"name\":\"mapfield\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
    org.apache.avro.Schema avroSchema = convertSchema(jsonSchema);

    Schema cdapSchema = Schema.recordOf("rec",
        Schema.Field.of(
            "mapfield",
            Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));

    Assert.assertEquals(avroToStructuredTransformer.convertSchema(avroSchema), cdapSchema);
  }

  @Test
  public void testAvroToStructuredConversionForEnums() throws IOException {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    String jsonSchema = "{\"type\":\"record\", \"name\":\"rec\"," +
        "\"namespace\": \"com.google.test\", \"fields\":[" +
        "{\"name\": \"enumfield\", \"type\": " +
        "{\"type\": \"enum\", \"name\": \"enumtype\", \"namespace\": \"com.google.test.enum\"," +
        "\"symbols\": [\"A\", \"B\", \"C\"]}}," +
        "{\"name\": \"arr\", \"type\": {\"type\": \"array\", " +
        "\"items\": \"com.google.test.enum.enumtype\"}}]}";
    org.apache.avro.Schema avroSchema = convertSchema(jsonSchema);

    Schema enumSchema = Schema.enumWith("A", "B", "C");
    Schema cdapSchema = Schema.recordOf("rec",
        Schema.Field.of("enumfield", enumSchema),
        Schema.Field.of("arr", Schema.arrayOf(enumSchema)));

    Assert.assertEquals(avroToStructuredTransformer.convertSchema(avroSchema), cdapSchema);
  }

  @Test
  public void testAvroToStructuredConversionForUnions() throws IOException {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    String jsonSchema = "{\"type\":\"record\", \"name\":\"com.google.test.rec\", \"fields\":[" +
        "{\"name\": \"enumfield\", \"type\": " +
        "{\"type\": \"enum\", \"name\": \"enumtype\", \"symbols\": [\"A\", \"B\", \"C\"]}}," +
        "{\"name\": \"unionfield\", \"type\": [\"null\", \"string\", \"enumtype\"," +
        "{\"type\": \"array\", \"items\": [\"int\", \"enumtype\"]}]}]}";
    org.apache.avro.Schema avroSchema = convertSchema(jsonSchema);

    Schema enumSchema = Schema.enumWith("A", "B", "C");
    Schema unionSchema = Schema.unionOf(
        Schema.of(Schema.Type.NULL),
        Schema.of(Schema.Type.STRING),
        enumSchema,
        Schema.arrayOf(Schema.unionOf(Schema.of(Schema.Type.INT), enumSchema)));
    Schema cdapSchema = Schema.recordOf("rec",
        Schema.Field.of("enumfield", enumSchema),
        Schema.Field.of("unionfield", unionSchema));

    Assert.assertEquals(avroToStructuredTransformer.convertSchema(avroSchema), cdapSchema);
  }

  @Test
  public void testAvroToStructuredConversionForMapsOfUnions() throws IOException {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    String jsonSchema = "{\"type\":\"record\", \"name\":\"rec\"," +
        "\"fields\":[{\"name\":\"mapfield\",\"type\":" +
        "{\"type\":\"map\",\"values\":[\"null\", \"string\", {\"type\": \"array\", \"items\": \"int\"}]}}]}";
    org.apache.avro.Schema avroSchema = convertSchema(jsonSchema);

    Schema unionSchema = Schema.unionOf(
        Schema.of(Schema.Type.NULL),
        Schema.of(Schema.Type.STRING),
        Schema.arrayOf(Schema.of(Schema.Type.INT)));
    Schema cdapSchema = Schema.recordOf("rec",
        Schema.Field.of(
            "mapfield",
            Schema.mapOf(Schema.of(Schema.Type.STRING), unionSchema)));

    Assert.assertEquals(avroToStructuredTransformer.convertSchema(avroSchema), cdapSchema);
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }

  private org.apache.avro.Schema convertSchema(String jsonSchema) {
    return new org.apache.avro.Schema.Parser().parse(jsonSchema);
  }
}
