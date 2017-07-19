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

/**
 * Tests {@link NullFieldSplitter}
 */
public class NullFieldSplitterTest {

  @Test
  public void testGetNonNullSchema() {
    Schema schema = Schema.recordOf("abc",
                                    Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("c",
                                                    Schema.unionOf(
                                                      Schema.of(Schema.Type.NULL),
                                                      Schema.of(Schema.Type.BYTES),
                                                      Schema.of(Schema.Type.INT),
                                                      Schema.of(Schema.Type.LONG))));

    Schema expected = Schema.recordOf("abc.nonnull",
                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                      Schema.Field.of("c",
                                                      Schema.unionOf(
                                                        Schema.of(Schema.Type.NULL),
                                                        Schema.of(Schema.Type.BYTES),
                                                        Schema.of(Schema.Type.INT),
                                                        Schema.of(Schema.Type.LONG))));
    Assert.assertEquals(expected, NullFieldSplitter.getNonNullSchema(schema, "a"));

    expected = Schema.recordOf("abc.nonull",
                               Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                               Schema.Field.of("b", Schema.of(Schema.Type.DOUBLE)),
                               Schema.Field.of("c",
                                               Schema.unionOf(
                                                 Schema.of(Schema.Type.NULL),
                                                 Schema.of(Schema.Type.BYTES),
                                                 Schema.of(Schema.Type.INT),
                                                 Schema.of(Schema.Type.LONG))));
    Assert.assertEquals(expected, NullFieldSplitter.getNonNullSchema(schema, "b"));

    expected = Schema.recordOf("abc.nonull",
                               Schema.Field.of("a", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                               Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                               Schema.Field.of("c",
                                               Schema.unionOf(
                                                 Schema.of(Schema.Type.BYTES),
                                                 Schema.of(Schema.Type.INT),
                                                 Schema.of(Schema.Type.LONG))));
    Assert.assertEquals(expected, NullFieldSplitter.getNonNullSchema(schema, "c"));
  }

  @Test
  public void testModifySchemaSplit() throws Exception {
    Schema nullSchema = Schema.recordOf("test",
                                         Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                         Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                         Schema.Field.of("z", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema nonNullSchema = Schema.recordOf("test",
                                           Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                           Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                           Schema.Field.of("z", Schema.of(Schema.Type.STRING)));
    NullFieldSplitter nullFieldSplitter = new NullFieldSplitter(new NullFieldSplitter.Conf("z", true));
    nullFieldSplitter.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> mockEmitter = new MockMultiOutputEmitter<>();

    // test non-null
    nullFieldSplitter.transform(StructuredRecord.builder(nullSchema).set("x", 0L).set("z", "abc").build(), mockEmitter);
    Assert.assertEquals(
      ImmutableMap.of(NullFieldSplitter.NON_NULL_PORT,
                      ImmutableList.of(StructuredRecord.builder(nonNullSchema).set("x", 0L).set("z", "abc").build())),
      mockEmitter.getEmitted());

    // test null
    mockEmitter.clear();
    nullFieldSplitter.transform(StructuredRecord.builder(nullSchema).set("y", 5).build(), mockEmitter);
    Assert.assertEquals(
      ImmutableMap.of(NullFieldSplitter.NULL_PORT,
                      ImmutableList.of(StructuredRecord.builder(nullSchema).set("y", 5).build())),
      mockEmitter.getEmitted());
  }

  @Test
  public void testSameSchemaSplit() throws Exception {
    Schema schema = Schema.recordOf("test",
                                        Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                        Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                        Schema.Field.of("z", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    NullFieldSplitter nullFieldSplitter = new NullFieldSplitter(new NullFieldSplitter.Conf("z", false));
    nullFieldSplitter.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> mockEmitter = new MockMultiOutputEmitter<>();

    // test non-null
    nullFieldSplitter.transform(StructuredRecord.builder(schema).set("x", 0L).set("z", "abc").build(), mockEmitter);
    Assert.assertEquals(
      ImmutableMap.of(NullFieldSplitter.NON_NULL_PORT,
                      ImmutableList.of(StructuredRecord.builder(schema).set("x", 0L).set("z", "abc").build())),
      mockEmitter.getEmitted());

    // test null
    mockEmitter.clear();
    nullFieldSplitter.transform(StructuredRecord.builder(schema).set("y", 5).build(), mockEmitter);
    Assert.assertEquals(
      ImmutableMap.of(NullFieldSplitter.NULL_PORT,
                      ImmutableList.of(StructuredRecord.builder(schema).set("y", 5).build())),
      mockEmitter.getEmitted());
  }

}
