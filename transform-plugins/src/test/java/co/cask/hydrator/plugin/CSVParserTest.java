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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.hydrator.common.MockPipelineConfigurer;
import co.cask.hydrator.common.test.MockEmitter;
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

  private static final Schema OUTPUT3 = Schema.recordOf("output3",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("f", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("g", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("h", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("i", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("j", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("k", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("l", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("m", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("n", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("o", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("p", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("r", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("s", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("t", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("u", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("v", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("w", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("x", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("y", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("z", Schema.of(Schema.Type.STRING)));

  @Test
  public void testDefaultCSVParser() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", "body", OUTPUT1.toString());
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
    CSVParser.Config config1 = new CSVParser.Config("DEFAULT", "body", OUTPUT2.toString());
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
    CSVParser.Config config = new CSVParser.Config("PDL", "body", OUTPUT1.toString());
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
  public void testPDLTypeChecks() throws Exception {
    CSVParser.Config config = new CSVParser.Config("PDL", "body", OUTPUT3.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body",
                               "4|14659|313420|34987647|2106005|168948698|2016-05-22|-1|-1|" +
                                 "-1|1|0|0|0|0|0|0|0|0|0|0|1|2|2016-05-30 20:30:37|0").build(), emitter);
    Assert.assertEquals(4L, emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(14659L, emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(313420L, emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(34987647L, emitter.getEmitted().get(0).get("d"));
  }

  @Test(expected = RuntimeException.class)
  public void testDoubleException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,3,,true").build(), emitter);
  }

  @Test(expected = RuntimeException.class)
  public void testIntException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,,4.32,true").build(), emitter);
  }

  @Test(expected = RuntimeException.class)
  public void testLongException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    CSVParser.Config config = new CSVParser.Config("DEFAULT", "body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", ",stringA,3,4.32,true").build(), emitter);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    CSVParser.Config config = new CSVParser.Config("DEFAULT", "body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT1, mockPipelineConfigurer.getOutputSchema());
  }

}
