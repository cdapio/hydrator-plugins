/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.mock.common.MockMultiOutputEmitter;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

    try {
      UnionSplitter.getOutputSchemas(inputSchema, "a", true);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      UnionSplitter.getOutputSchemas(inputSchema, "b", true);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      UnionSplitter.getOutputSchemas(inputSchema, "c", true);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    UnionSplitter.getOutputSchemas(inputSchema, "d", true);
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
    expected.put("float", Schema.recordOf("union.double",
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
    Map<String, Schema> actual = UnionSplitter.getOutputSchemas(inputSchema, "b", true);
    Assert.assertEquals(expected, actual);

    // test without schema modification
    expected.clear();
    expected.put("null", inputSchema);
    expected.put("boolean", inputSchema);
    expected.put("bytes", inputSchema);
    expected.put("int", inputSchema);
    expected.put("long", inputSchema);
    expected.put("float", inputSchema);
    expected.put("double", inputSchema);
    expected.put("string", inputSchema);
    expected.put("rec1", inputSchema);
    expected.put("rec2", inputSchema);
    actual = UnionSplitter.getOutputSchemas(inputSchema, "b", false);
    Assert.assertEquals(expected, actual);
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
    Schema floatSchema = Schema.recordOf("union.double",
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
    unionSplitter.initialize(new MockTransformContext());

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
  }
}
