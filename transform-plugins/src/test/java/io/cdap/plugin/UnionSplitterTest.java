/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link UnionSplitter}
 */
public class UnionSplitterTest {

  @Test
  public void testInvalidSchemas() {
    Schema inputSchema = Schema.recordOf(
      "union",
      Schema.Field.of("a", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.enumWith("something"))),
      Schema.Field.of("c", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("d", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    FailureCollector collector = new MockFailureCollector();

    UnionSplitter.getOutputSchemas(inputSchema, "a", true, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(2, collector.getValidationFailures().get(0).getCauses().size());

    collector = new MockFailureCollector();
    UnionSplitter.getOutputSchemas(inputSchema, "b", true, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(2, collector.getValidationFailures().get(0).getCauses().size());

    collector = new MockFailureCollector();
    UnionSplitter.getOutputSchemas(inputSchema, "c", true, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(2, collector.getValidationFailures().get(0).getCauses().size());

    collector = new MockFailureCollector();
    UnionSplitter.getOutputSchemas(inputSchema, "d", true, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testOutputSchemas() {
    Schema rec1Schema = Schema.recordOf(
      "rec1",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.INT)));
    Schema rec2Schema = Schema.recordOf(
      "rec2",
      Schema.Field.of("y", Schema.of(Schema.Type.INT)),
      Schema.Field.of("z", Schema.of(Schema.Type.INT)));

    Schema inputSchema = Schema.recordOf("union",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                                                             Schema.of(Schema.Type.BOOLEAN),
                                                                             Schema.of(Schema.Type.BYTES),
                                                                             Schema.of(Schema.Type.INT),
                                                                             Schema.of(Schema.Type.LONG),
                                                                             Schema.of(Schema.Type.FLOAT),
                                                                             Schema.of(Schema.Type.DOUBLE),
                                                                             Schema.of(Schema.Type.STRING),
                                                                             rec1Schema, rec2Schema)));

    // test with schema modification
    Map<String, Schema> expected = new HashMap<>();
    expected.put("null", Schema.recordOf("union.null",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", Schema.of(Schema.Type.NULL))));
    expected.put("boolean", Schema.recordOf("union.boolean",
                                            Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("b", Schema.of(Schema.Type.BOOLEAN))));
    expected.put("bytes", Schema.recordOf("union.bytes",
                                          Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("b", Schema.of(Schema.Type.BYTES))));
    expected.put("int", Schema.recordOf("union.int",
                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("b", Schema.of(Schema.Type.INT))));
    expected.put("long", Schema.recordOf("union.long",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", Schema.of(Schema.Type.LONG))));
    expected.put("float", Schema.recordOf("union.float",
                                           Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                           Schema.Field.of("b", Schema.of(Schema.Type.FLOAT))));
    expected.put("double", Schema.recordOf("union.double",
                                           Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                           Schema.Field.of("b", Schema.of(Schema.Type.DOUBLE))));
    expected.put("string", Schema.recordOf("union.string",
                                           Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                           Schema.Field.of("b", Schema.of(Schema.Type.STRING))));
    expected.put("rec1", Schema.recordOf("union.rec1",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", rec1Schema)));
    expected.put("rec2", Schema.recordOf("union.rec2",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", rec2Schema)));
    FailureCollector collector = new MockFailureCollector();
    Map<String, Schema> actual = UnionSplitter.getOutputSchemas(inputSchema, "b", true, collector);
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(0, collector.getValidationFailures().size());

    // test without schema modification
    expected.clear();
    expected.put("null", changeName(inputSchema, "union.null"));
    expected.put("boolean", changeName(inputSchema, "union.boolean"));
    expected.put("bytes", changeName(inputSchema, "union.bytes"));
    expected.put("int", changeName(inputSchema, "union.int"));
    expected.put("long", changeName(inputSchema, "union.long"));
    expected.put("float", changeName(inputSchema, "union.float"));
    expected.put("double", changeName(inputSchema, "union.double"));
    expected.put("string", changeName(inputSchema, "union.string"));
    expected.put("rec1", changeName(inputSchema, "union.rec1"));
    expected.put("rec2", changeName(inputSchema, "union.rec2"));
    actual = UnionSplitter.getOutputSchemas(inputSchema, "b", false, collector);
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private Schema changeName(Schema schema, String name) {
    return Schema.recordOf(name, schema.getFields());
  }

  @Test
  public void testSplitOnField() throws Exception {
    Schema rec1Schema = Schema.recordOf(
      "rec1",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.INT)));
    Schema rec2Schema = Schema.recordOf(
      "rec2",
      Schema.Field.of("y", Schema.of(Schema.Type.INT)),
      Schema.Field.of("z", Schema.of(Schema.Type.INT)));
    StructuredRecord rec1 = StructuredRecord.builder(rec1Schema).set("x", 0).set("y", 1).build();
    StructuredRecord rec2 = StructuredRecord.builder(rec2Schema).set("y", 2).set("z", 3).build();

    Schema inputSchema = Schema.recordOf(
      "union",
      Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                          Schema.of(Schema.Type.BOOLEAN),
                                          Schema.of(Schema.Type.INT),
                                          Schema.of(Schema.Type.LONG),
                                          Schema.of(Schema.Type.FLOAT),
                                          Schema.of(Schema.Type.DOUBLE),
                                          Schema.of(Schema.Type.STRING),
                                          rec1Schema, rec2Schema)));

    Schema nullSchema = Schema.recordOf("union.null",
                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("b", Schema.of(Schema.Type.NULL)));
    Schema boolSchema = Schema.recordOf("union.boolean",
                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("b", Schema.of(Schema.Type.BOOLEAN)));
    Schema intSchema = Schema.recordOf("union.int",
                                       Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                       Schema.Field.of("b", Schema.of(Schema.Type.INT)));
    Schema longSchema = Schema.recordOf("union.long",
                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("b", Schema.of(Schema.Type.LONG)));
    Schema floatSchema = Schema.recordOf("union.float",
                                         Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                         Schema.Field.of("b", Schema.of(Schema.Type.FLOAT)));
    Schema doubleSchema = Schema.recordOf("union.double",
                                          Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("b", Schema.of(Schema.Type.DOUBLE)));
    Schema stringSchema = Schema.recordOf("union.string",
                                          Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
    Schema withRec1Schema = Schema.recordOf("union.rec1",
                                            Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("b", rec1Schema));
    Schema withRec2Schema = Schema.recordOf("union.rec2",
                                            Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("b", rec2Schema));

    UnionSplitter unionSplitter = new UnionSplitter(new UnionSplitter.Conf("b", true));
    TransformContext context = new MockTransformContext();
    unionSplitter.initialize(context);

    MockMultiOutputEmitter<StructuredRecord> mockEmitter = new MockMultiOutputEmitter<>();
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", null)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", true)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", new byte[] { 5 })
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", 5)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", 5L)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", 5.5f)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", 5.5d)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", "5")
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", rec1)
                              .build(),
                            mockEmitter);
    unionSplitter.transform(StructuredRecord.builder(inputSchema)
                              .set("a", 0L)
                              .set("b", rec2)
                              .build(),
                            mockEmitter);

    Map<String, List<StructuredRecord>> expected = new HashMap<>();
    expected.put("null", ImmutableList.of(StructuredRecord.builder(nullSchema)
                                            .set("a", 0L)
                                            .set("b", null)
                                            .build()));
    expected.put("boolean", ImmutableList.of(StructuredRecord.builder(boolSchema)
                                            .set("a", 0L)
                                            .set("b", true)
                                            .build()));
    expected.put("int", ImmutableList.of(StructuredRecord.builder(intSchema)
                                            .set("a", 0L)
                                            .set("b", 5)
                                            .build()));
    expected.put("long", ImmutableList.of(StructuredRecord.builder(longSchema)
                                            .set("a", 0L)
                                            .set("b", 5L)
                                            .build()));
    expected.put("float", ImmutableList.of(StructuredRecord.builder(floatSchema)
                                            .set("a", 0L)
                                            .set("b", 5.5f)
                                            .build()));
    expected.put("double", ImmutableList.of(StructuredRecord.builder(doubleSchema)
                                            .set("a", 0L)
                                            .set("b", 5.5d)
                                            .build()));
    expected.put("string", ImmutableList.of(StructuredRecord.builder(stringSchema)
                                            .set("a", 0L)
                                            .set("b", "5")
                                            .build()));
    expected.put("rec1", ImmutableList.of(StructuredRecord.builder(withRec1Schema)
                                            .set("a", 0L)
                                            .set("b", rec1)
                                            .build()));
    expected.put("rec2", ImmutableList.of(StructuredRecord.builder(withRec2Schema)
                                            .set("a", 0L)
                                            .set("b", rec2)
                                            .build()));

    Map<String, List<Object>> actual = mockEmitter.getEmitted();
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(0, context.getFailureCollector().getValidationFailures().size());
  }
}
