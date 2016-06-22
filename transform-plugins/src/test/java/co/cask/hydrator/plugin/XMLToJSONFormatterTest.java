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
 * Tests {@link JSONParser}
 */
public class XMLToJSONFormatterTest {
  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT2 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  @Test
  public void testJSONParser() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "{\"a\": \"1\", \"b\": \"2\", \"c\" : \"3\", \"d\" : \"4\", \"e\" : \"5\" }")
                          .build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testJSONParserProjections() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "{\"a\": \"1\", \"b\": \"2\", \"c\" : \"3\", \"d\" : \"4\", \"e\" : \"5\" }")
                          .build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSchemaInvalidSchema() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body2", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT2, mockPipelineConfigurer.getOutputSchema());
  }
}
