/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import co.cask.hydrator.plugin.validator.CoreValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link PythonEvaluator}.
 */
public class PythonEvaluatorTest {

  private static final Schema SCHEMA = Schema.recordOf("record",
    Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
    Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("nullableField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("mapField", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
    Schema.Field.of("arrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("unionField", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))));
  private static final StructuredRecord RECORD1 = StructuredRecord.builder(SCHEMA)
    .set("booleanField", true)
    .set("intField", 28)
    .set("longField", 99L)
    .set("floatField", 2.71f)
    .set("doubleField", 3.14)
    .set("bytesField", Bytes.toBytes("foo"))
    .set("stringField", "bar")
    .set("nullableField", "baz")
    .set("mapField", ImmutableMap.of("foo", 13, "bar", 17))
    .set("arrayField", ImmutableList.of("foo", "bar", "baz"))
    .set("unionField", "hello")
    .build();
  private static final StructuredRecord RECORD2 = StructuredRecord.builder(SCHEMA)
    .set("booleanField", false)
    .set("intField", -28)
    .set("longField", -99L)
    .set("floatField", -2.71f)
    .set("doubleField", -3.14)
    .set("bytesField", Bytes.toBytes("hello"))
    .set("stringField", "world")
    .set("nullableField", null)
    .set("mapField", ImmutableMap.of())
    .set("arrayField", ImmutableList.of())
    .set("unionField", 3)
    .build();

  @Test
  public void testSimple() throws Exception {
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
      "  x['intField'] *= 1024\n" +
      "  emitter.emit(x)\n" +
      "  emitter.emit(x)",
      null);
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    // check record1
    Assert.assertEquals(SCHEMA, output.getSchema());
    Assert.assertTrue(output.get("booleanField"));
    Assert.assertEquals(28 * 1024, output.<Integer>get("intField").intValue());
    Assert.assertEquals(99L, output.<Long>get("longField").longValue());
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
    Assert.assertEquals("baz", output.get("nullableField"));
    Assert.assertEquals("hello", output.get("unionField"));
    Map<String, Integer> expectedMapField = ImmutableMap.of("foo", 13, "bar", 17);
    List<String> expectedListField = ImmutableList.of("foo", "bar", "baz");
    Assert.assertEquals(expectedMapField, output.get("mapField"));
    Assert.assertEquals(expectedListField, output.get("arrayField"));
    emitter.clear();

    // check record2
    transform.transform(RECORD2, emitter);
    output = emitter.getEmitted().get(0);

    // we emit the same record twice, to test that functionality
    Assert.assertEquals(output, emitter.getEmitted().get(1));

    Assert.assertEquals(SCHEMA, output.getSchema());
    Assert.assertFalse(output.get("booleanField"));
    Assert.assertEquals(-28 * 1024, output.<Integer>get("intField").intValue());
    Assert.assertEquals(-99L, output.<Long>get("longField").longValue());
    Assert.assertTrue(Math.abs(-2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(-3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("hello"), output.get("bytesField"));
    Assert.assertEquals("world", output.get("stringField"));
    Assert.assertNull(output.get("nullableField"));
    Assert.assertEquals(3, output.<Integer>get("unionField").intValue());
    expectedMapField = ImmutableMap.of();
    expectedListField = ImmutableList.of();
    Assert.assertEquals(expectedMapField, output.get("mapField"));
    Assert.assertEquals(expectedListField, output.get("arrayField"));
  }

  @Test
  public void testArguments() throws Exception {
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(input, emitter, context):\n" +
        "  multiplier = int(context.getArguments().get('multiplier'))\n" +
        "  emitter.emit({ 'x': input['x'] * multiplier })",
      null);
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext("stage", ImmutableMap.of("multiplier", "5")));

    Schema schema = Schema.recordOf("x", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(schema).set("x", 2).build(), emitter);
    Assert.assertEquals(ImmutableList.of(StructuredRecord.builder(schema).set("x", 10).build()),
                        emitter.getEmitted());
  }

  @Test
  public void testDecodingSimpleTypes() throws Exception {
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
        "  x['floatField'] *= 1.01\n" +
        "  x['doubleField'] *= 1.01\n" +
        "  x['longField'] *= 2\n" +
        "  emitter.emit(x)",
      null);
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    // check simple types are decoded properly
    Assert.assertEquals(SCHEMA, output.getSchema());
    Assert.assertEquals(198L, output.<Long>get("longField").longValue());
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.03);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.032);
    emitter.clear();
  }

  @Test
  public void testNewOutputEmit() throws Exception {
    Schema floatSchema = Schema.recordOf("record", Schema.Field.of("body", Schema.of(Schema.Type.FLOAT)));
    StructuredRecord record3 = StructuredRecord.builder(floatSchema).set("body", 2.71f).build();
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(record, emitter, context):\n" +
        "  output = {}\n" +
        "  output['body'] = record['body']\n" +
        "  emitter.emit(output)",
      null);
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext());
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(record3, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    // check that output is correct
    Assert.assertEquals(floatSchema, output.getSchema());
    Assert.assertEquals(2.71f, output.get("body"), 0.000001f);
    emitter.clear();
  }

  @Test(expected = Exception.class)
  public void testScriptCompilationValidation() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)));

    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
        " y = {}\n" +
        "y['intField'] *= 1024\n" +
        " y['longField'] *= 1024\n" +
        " emitter.emit(y)",
      outputSchema.toString());

    Schema inputSchema = Schema.recordOf(
      "biggerSchema",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema,
                                                                   ImmutableMap.of(
                                                                     CoreValidator.ID, new CoreValidator()));
    new PythonEvaluator(config).configurePipeline(configurer);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)));

    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
        " y = {}\n" +
        " y['intField'] *= 1024\n" +
        " y['longField'] *= 1024\n" +
        " emitter.emit(y)",
      outputSchema.toString());

    Schema inputSchema = Schema.recordOf(
      "biggerSchema",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    new PythonEvaluator(config).configurePipeline(configurer);
    Assert.assertEquals(outputSchema, configurer.getOutputSchema());

    // check if schema is null, input schema is used.
    config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
        "  y = {}\n" +
        "  y['intField'] *= 1024\n" +
        "  y['longField'] *= 1024\n" +
        "  emitter.emit(y)",
      null);
    new PythonEvaluator(config).configurePipeline(configurer);
    Assert.assertEquals(inputSchema, configurer.getOutputSchema());
  }

  @Test
  public void testEmitError() throws Exception {
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(x, emitter, context):\n" +
        "  emitter.emitError({\"errorCode\":31, \"errorMsg\":\"error!\", \"invalidRecord\": x})",
      null);
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
    Assert.assertEquals(31, invalidEntry.getErrorCode());
    Assert.assertEquals("error!", invalidEntry.getErrorMsg());
    Assert.assertEquals(RECORD1, invalidEntry.getInvalidRecord());
  }

  @Test
  public void testDropAndRename() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)));
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(input, emitter, context): emitter.emit({ 'x':input['intField'], 'y':input['longField'] })",
      outputSchema.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals(outputSchema, output.getSchema());
    Assert.assertEquals(28, output.<Integer>get("x").intValue());
    Assert.assertEquals(99L, output.<Long>get("y").longValue());
  }

  @Test
  public void testComplex() throws Exception {
    Schema inner2Schema = Schema.recordOf(
      "inner2",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("val", Schema.of(Schema.Type.DOUBLE))
    );
    Schema inner1Schema = Schema.recordOf(
      "inner1",
      Schema.Field.of("list",
        Schema.arrayOf(Schema.mapOf(
                         Schema.of(Schema.Type.STRING), inner2Schema)
      ))
    );
    Schema schema = Schema.recordOf(
      "complex",
      Schema.Field.of("num", Schema.of(Schema.Type.INT)),
      Schema.Field.of("inner1", inner1Schema)
    );
/*
    {
      "complex": {
        "num": 8,
        "inner1": {
          "list": [
            "map": {
              "p": {
                "name": "pi",
                "val": 3.14
              },
              "e": {
                "name": "e",
                "val": 2.71
              }
            }
          ]
        }
      }
    }
    */

    StructuredRecord pi = StructuredRecord.builder(inner2Schema).set("name", "pi").set("val", 3.14).build();
    StructuredRecord e = StructuredRecord.builder(inner2Schema).set("name", "e").set("val", 2.71).build();
    StructuredRecord inner1 = StructuredRecord.builder(inner1Schema)
      .set("list", Lists.newArrayList(ImmutableMap.of("p", pi, "e", e)))
      .build();
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("num", 8)
      .set("inner1", inner1)
      .build();

    Schema outputSchema = Schema.recordOf("output", Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)));
    PythonEvaluator.Config config = new PythonEvaluator.Config(
      "def transform(input, emitter, context):\n" +
        "  pi = input['inner1']['list'][0]['p']\n" +
        "  e = input['inner1']['list'][0]['e']\n" +
        "  val = power(pi['val'], 3) + power(e['val'], 2)\n" +
        "  print(pi); print(e); print(context);\n" +
        "  context.getMetrics().count(\"script.transform.count\", 1)\n" +
        "  context.getLogger().info(\"Log test from Script Transform\")\n" +
        "  emitter.emit({ 'x':val })\n" +
        "" +
        "def power(x, y):\n" +
        "  ans = 1\n" +
        "  for i in range(y):\n" +
        "    ans = ans * x\n" +
        "  return ans\n" +
        "",
      outputSchema.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new PythonEvaluator(config);
    MockTransformContext mockContext = new MockTransformContext();
    transform.initialize(mockContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals(outputSchema, output.getSchema());
    Assert.assertTrue(Math.abs(2.71 * 2.71 + 3.14 * 3.14 * 3.14 - (Double) output.get("x")) < 0.000001);
    Assert.assertEquals(1, mockContext.getMockMetrics().getCount("script.transform.count"));
  }
}
