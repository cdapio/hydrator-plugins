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
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import com.google.common.base.Joiner;
import com.jayway.jsonpath.PathNotFoundException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link JSONParser}
 */
public class JSONParserTest {
  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT2 = Schema.recordOf("output2",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT3 = Schema.recordOf("output3",
                                                        Schema.Field.of("expensive", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("bicycle_color", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("bicycle_price", Schema.of(Schema.Type.FLOAT)));

  private static final Schema OUTPUT4 = Schema.recordOf("output4",
                                                        Schema.Field.of("expensive", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("bicycle_color", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("bicycle_price", Schema.of(Schema.Type.FLOAT)),
                                                        Schema.Field.of("window", Schema.of(Schema.Type.FLOAT)));

  private static final String json = "{\n" +
    "    \"store\": {\n" +
    "        \"book\": [\n" +
    "            {\n" +
    "                \"category\": \"reference\",\n" +
    "                \"author\": \"Nigel Rees\",\n" +
    "                \"title\": \"Sayings of the Century\",\n" +
    "                \"price\": 8.95\n" +
    "            },\n" +
    "            {\n" +
    "                \"category\": \"fiction\",\n" +
    "                \"author\": \"Evelyn Waugh\",\n" +
    "                \"title\": \"Sword of Honour\",\n" +
    "                \"price\": 12.99\n" +
    "            },\n" +
    "            {\n" +
    "                \"category\": \"fiction\",\n" +
    "                \"author\": \"Herman Melville\",\n" +
    "                \"title\": \"Moby Dick\",\n" +
    "                \"isbn\": \"0-553-21311-3\",\n" +
    "                \"price\": 8.99\n" +
    "            },\n" +
    "            {\n" +
    "                \"category\": \"fiction\",\n" +
    "                \"author\": \"J. R. R. Tolkien\",\n" +
    "                \"title\": \"The Lord of the Rings\",\n" +
    "                \"isbn\": \"0-395-19395-8\",\n" +
    "                \"price\": 22.99\n" +
    "            }\n" +
    "        ],\n" +
    "        \"bicycle\": {\n" +
    "            \"color\": \"red\",\n" +
    "            \"price\": 19.95\n" +
    "        }\n" +
    "    },\n" +
    "    \"expensive\": 10\n" +
    "}";

  @Test
  public void testJSONParser() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", "", OUTPUT1.toString());
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
    JSONParser.Config config = new JSONParser.Config("body", "", OUTPUT2.toString());
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
    JSONParser.Config config = new JSONParser.Config("body2", "", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", "", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT2, mockPipelineConfigurer.getOutputSchema());
  }

  @Test
  public void testComplexJSONParsing() throws Exception {
    final String[] jsonPaths = {
      "expensive:$.expensive",
      "bicycle_color:$.store.bicycle.color",
      "bicycle_price:$.store.bicycle.price",
      "window:$.store.window"
    };

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    JSONParser.Config config = new JSONParser.Config("body", Joiner.on(",").join(jsonPaths),
                                                     OUTPUT3.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", json)
                          .build(), emitter);
    Assert.assertEquals(10, emitter.getEmitted().get(0).get("expensive"));
    Assert.assertEquals("red", emitter.getEmitted().get(0).get("bicycle_color"));
    Assert.assertEquals(19.95, emitter.getEmitted().get(0).get("bicycle_price"));
  }

  @Test(expected = PathNotFoundException.class)
  public void testInvalidJsonPath() throws Exception {
    final String[] jsonPaths = {
      "expensive:$.expensive",
      "bicycle_color:$.store.bicycle.color",
      "bicycle_price:$.store.bicycle.price",
      "window:$.store.window"
    };

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    JSONParser.Config config = new JSONParser.Config("body", Joiner.on(",").join(jsonPaths),
                                                     OUTPUT4.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", json)
                          .build(), emitter);
  }
}
