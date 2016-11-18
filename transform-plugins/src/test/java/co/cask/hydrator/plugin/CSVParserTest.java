/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests the {@link CSVParser}.
 */
public class CSVParserTest {

  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT2 = Schema.recordOf("output2",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)));

  // Input record for pass through.
  private static final Schema INPUT2 = Schema.recordOf("input2",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));

  // Correct schema type pass through for output schema.
  private static final Schema OUTPUT3 = Schema.recordOf("output3",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)),
                                                        Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));

  // Wrong schema type pass through for output schema.
  private static final Schema OUTPUT4 = Schema.recordOf("output4",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)),
                                                        Schema.Field.of("offset", Schema.of(Schema.Type.INT)));

  // Input schema with nullable field to parse
  private static final Schema NULLABLE_INPUT = Schema.recordOf("nullableInput",
                                                               Schema.Field.of("body", Schema.nullableOf(
                                                                 Schema.of(Schema.Type.STRING))));

  @Test
  public void testNullableFields() throws Exception {
    Schema schema = Schema.recordOf("nullables",
                                    Schema.Field.of("int", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("float", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("double", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("bool", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("string", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", schema.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    transform.transform(StructuredRecord.builder(INPUT1).set("body", "1,2,3,4,true,abc").build(), emitter);
    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("int", 1)
      .set("long", 2L)
      .set("float", 3f)
      .set("double", 4d)
      .set("bool", true)
      .set("string", "abc")
      .build();
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(expected, emitter.getEmitted().get(0));

    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1).set("body", ",,,,,").build(), emitter);
    expected = StructuredRecord.builder(schema)
      .set("int", null)
      .set("long", null)
      .set("float", null)
      .set("double", null)
      .set("bool", null)
      .set("string", "")
      .build();
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(expected, emitter.getEmitted().get(0));
  }

  @Test
  public void testDefaultCSVParser() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("", emitter.getEmitted().get(0).get("e"));

    // Test adding quote to field value.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,'4',5").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("'4'", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));

    // Test adding spaces in a field and quoted field value.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2, 3 ,'4',5").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(" 3 ", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("'4'", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));

    // Test Skipping empty lines.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,5\n\n").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
    Assert.assertEquals(1, emitter.getEmitted().size());

    // Test multiple records
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,5\n6,7,8,9,10").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
    Assert.assertEquals("6", emitter.getEmitted().get(1).get("a"));
    Assert.assertEquals("7", emitter.getEmitted().get(1).get("b"));
    Assert.assertEquals("8", emitter.getEmitted().get(1).get("c"));
    Assert.assertEquals("9", emitter.getEmitted().get(1).get("d"));
    Assert.assertEquals("10", emitter.getEmitted().get(1).get("e"));

    // Test with records supporting different types.
    emitter.clear();
    CSVParser.Config config1 = new CSVParser.Config("DEFAULT", null, "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform1 = new CSVParser(config1);
    transform1.initialize(null);

    transform1.transform(StructuredRecord.builder(INPUT1)
                           .set("body", "10,stringA,3,4.32,true").build(), emitter);
    Assert.assertEquals(10L, emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("stringA", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(3, emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4.32, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(true, emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testPDL() throws Exception {
    CSVParser.Config config = new CSVParser.Config("PDL", null, "body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1|    2|3 |4|        ").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("", emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testCustomDelimiter() throws Exception {
    CSVParser.Config config = new CSVParser.Config("Custom", ';', "body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1;    2;3 ;4;        ").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("", emitter.getEmitted().get(0).get("e"));
  }

  @Test(expected = RuntimeException.class)
  public void testDoubleException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,3,,true").build(), emitter);
  }

  @Test(expected = RuntimeException.class)
  public void testIntException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,,4.32,true").build(), emitter);
  }

  @Test(expected = RuntimeException.class)
  public void testLongException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", ",stringA,3,4.32,true").build(), emitter);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT1.toString());
    CSVParser csvParser = new CSVParser(config);
    csvParser.validateInputSchema(INPUT1);
    Assert.assertEquals(OUTPUT1, csvParser.parseAndValidateOutputSchema(INPUT1));
  }

  @Test
  public void testNullableFieldSchemaValidation() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT1.toString());
    CSVParser csvParser = new CSVParser(config);
    csvParser.validateInputSchema(NULLABLE_INPUT);
    Assert.assertEquals(OUTPUT1, csvParser.parseAndValidateOutputSchema(NULLABLE_INPUT));
  }

  @Test
  public void testPassThrough() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT3.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT2);
    transform.configurePipeline(mockPipelineConfigurer);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT2)
                          .set("body", "10,stringA,3,4.32,true").set("offset", 10).build(), emitter);
    Assert.assertEquals(10L, emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("stringA", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(3, emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4.32, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(true, emitter.getEmitted().get(0).get("e"));
    Assert.assertEquals(10, emitter.getEmitted().get(0).get("offset")); // Pass through from input.
  }

  @Test (expected = IllegalArgumentException.class)
  public void testPassThroughTypeMisMatch() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", null, "body", OUTPUT4.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT2);
    transform.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testEmptyCustomDelimiter() throws Exception {
    CSVParser config = new CSVParser(new CSVParser.Config("Custom", null, "body", OUTPUT4.toString()));
    try {
      config.configurePipeline(new MockPipelineConfigurer(INPUT2));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Please specify the delimiter for format option 'Custom'.", e.getMessage());
    }
  }
}
