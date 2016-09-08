/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupTableConfig;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockLookupProvider;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import co.cask.hydrator.plugin.validator.CoreValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test case for {@link JavaScriptTransform}.
 */
public class JavaScriptTransformTest {

  private static final Lookup<String> TEST_LOOKUP = new Lookup<String>() {
    @Override
    public String lookup(String key) {
      return key;
    }

    @Override
    public Map<String, String> lookup(String... keys) {
      Map<String, String> result = new HashMap<>();
      for (String key : keys) {
        result.put(key, key);
      }
      return result;
    }

    @Override
    public Map<String, String> lookup(Set<String> keys) {
      Map<String, String> result = new HashMap<>();
      for (String key : keys) {
        result.put(key, key);
      }
      return result;
    }
  };

  private static final Schema SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
                    Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
                    Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("nullableField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("mapField", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                             Schema.of(Schema.Type.INT))),
                    Schema.Field.of("arrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("unionField", Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                 Schema.of(Schema.Type.INT))));
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

  private static final Schema STRING_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));
  private static final StructuredRecord STRING_RECORD = StructuredRecord.builder(STRING_SCHEMA)
    .set("stringField", "zzz")
    .build();

  @Test
  public void testSimple() throws Exception {
    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(x, emitter, context) { x.intField = x.intField * 1024; emitter.emit(x); }", null, null);
    Transform<StructuredRecord, StructuredRecord> transform = new JavaScriptTransform(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    // check record1
    Assert.assertEquals(SCHEMA, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28 * 1024, output.get("intField"));
    Assert.assertEquals(99L, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
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
    Assert.assertEquals(SCHEMA, output.getSchema());
    Assert.assertFalse((Boolean) output.get("booleanField"));
    Assert.assertEquals(-28 * 1024, output.get("intField"));
    Assert.assertEquals(-99L, output.get("longField"));
    Assert.assertTrue(Math.abs(-2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(-3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("hello"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("world", output.get("stringField"));
    Assert.assertNull(output.get("nullableField"));
    Assert.assertEquals(3, output.get("unionField"));
    expectedMapField = ImmutableMap.of();
    expectedListField = ImmutableList.of();
    Assert.assertEquals(expectedMapField, output.get("mapField"));
    Assert.assertEquals(expectedListField, output.get("arrayField"));
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)));
    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(input, emitter, context) { emitter.emit({ 'x':input.intField, 'y':input.longField }); }",
      outputSchema.toString(), null);

    Schema inputSchema = Schema.recordOf(
      "biggerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE)));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema,
                                                                   ImmutableMap.<String, Object>of(
                                                                     CoreValidator.ID, new CoreValidator()));
    new JavaScriptTransform(config).configurePipeline(configurer);
    Assert.assertEquals(outputSchema, configurer.getOutputSchema());

    // if schame is null in config, then input schema is set as output schema
    config = new JavaScriptTransform.Config(
      "function transform(input, emitter, context) { emitter.emit({ 'x':input.intField, 'y':input.longField }); }",
      null, null);

    new JavaScriptTransform(config).configurePipeline(configurer);
    Assert.assertEquals(inputSchema, configurer.getOutputSchema());

  }

  @Test
  public void testLookup() throws Exception {
    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(x, emitter, ctx) { " +
        "var single = ctx.getLookup('purchases').lookup('abc');" +
        "var batch = ctx.getLookup('purchases').lookup(['abc', 'sdf']);" +
        "x.stringField = '1_' + single + ' 2_' + batch['abc'] + batch['sdf'] + '::' + batch.abc;" +
        "emitter.emit(x);" +
        "}",
      null,
      new LookupConfig(
        ImmutableMap.of(
          "purchases", new LookupTableConfig(LookupTableConfig.TableType.DATASET))
      ));
    Transform<StructuredRecord, StructuredRecord> transform = new JavaScriptTransform(config);
    transform.initialize(new MockTransformContext("somestage",
                                                  new HashMap<String, String>(),
                                                  new MockLookupProvider(TEST_LOOKUP)));

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(STRING_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    // check record1
    Assert.assertEquals(STRING_SCHEMA, output.getSchema());
    Assert.assertEquals("1_abc 2_abcsdf::abc", output.get("stringField"));
  }

  @Test
  public void testEmitErrors() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)));

    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(input, emitter, context) { " +
        "emitter.emitError({\"errorCode\":31, \"errorMsg\":\"error!\", \"invalidRecord\": input}); }",
       outputSchema.toString(), null);
    Transform<StructuredRecord, StructuredRecord> transform = new JavaScriptTransform(config);
    transform.initialize(new MockTransformContext());

    StructuredRecord inputRecord = StructuredRecord.builder(outputSchema)
      .set("x", 25)
      .set("y", 28L)
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(inputRecord, emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    Assert.assertEquals(1, emitter.getErrors().size());

    InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
    Assert.assertEquals(31, invalidEntry.getErrorCode());
    Assert.assertEquals("error!", invalidEntry.getErrorMsg());
    Assert.assertEquals(25, invalidEntry.getInvalidRecord().get("x"));
    Assert.assertEquals(28L, invalidEntry.getInvalidRecord().get("y"));
  }

  @Test
  public void testDropAndRename() throws Exception {
    Schema outputSchema = Schema.recordOf(
      "smallerSchema",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.LONG)));
    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(input, emitter, context) { var returnMap = {}; " +
        "returnMap['x'] = input.intField; " +
        "returnMap['y'] = input.longField; " +
        "emitter.emit(returnMap); }",
      outputSchema.toString(), null);
    Transform<StructuredRecord, StructuredRecord> transform = new JavaScriptTransform(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals(outputSchema, output.getSchema());
    Assert.assertEquals(28, output.get("x"));
    Assert.assertEquals(99L, output.get("y"));
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
    JavaScriptTransform.Config config = new JavaScriptTransform.Config(
      "function transform(input, emitter, context) {\n" +
        "  var pi = input.inner1.list[0].p;\n" +
        "  var e = input.inner1.list[0].e;\n" +
        "  var val = power(pi.val, 3) + power(e.val, 2);\n" +
        "  print(pi); print(e);\n print(context);\n" +
        "  context.getMetrics().count(\"script.transform.count\", 1);\n" +
        "  context.getMetrics().pipelineCount(\"script.transform.count\", 1);\n" +
        "  context.getLogger().info(\"Log test from Script Transform\");\n" +
        "  emitter.emit({ 'x':val });\n" +
        "}" +
        "function power(x, y) { \n" +
        "  var ans = 1; \n" +
        "  for (i = 0; i < y; i++) { \n" +
        "    ans = ans * x;\n" +
        "  }\n" +
        "  return ans;\n" +
        "}",
      outputSchema.toString(), null);
    Transform<StructuredRecord, StructuredRecord> transform = new JavaScriptTransform(config);
    MockTransformContext mockContext = new MockTransformContext("transform.1");
    transform.initialize(mockContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
    Assert.assertEquals(outputSchema, output.getSchema());
    Assert.assertTrue(Math.abs(2.71 * 2.71 + 3.14 * 3.14 * 3.14 - (Double) output.get("x")) < 0.000001);
    Assert.assertEquals(1, mockContext.getMockMetrics().getCount("script.transform.count"));
    Assert.assertEquals(1, mockContext.getMockMetrics().getPipelineCount("transform.1.script.transform.count"));
  }
}
