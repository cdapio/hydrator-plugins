/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 */
public class XMLMultiParserTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testMultipleRecordsFromOne() throws Exception {
    // String field, String encoding, String xPath, String schema
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("desc", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord input = StructuredRecord.builder(inputSchema)
      .set("body",
           "<items>" +
             "<item><id>0</id><name>Burrito</name><price>7.77</price><desc>delicious</desc></item>" +
             "<item><id>100</id><name>Tortilla Chips</name><price>0.99</price></item>" +
             "<item><id>200</id><name>Water</name><price>2.99</price></item>" +
             "</items>")
      .build();

    XMLMultiParser parser = new XMLMultiParser(config);
    parser.initialize(new MockTransformContext("stage"));
    parser.transform(input, emitter);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("id", 0L).set("name", "Burrito").set("price", 7.77d).set("desc", "delicious").build(),
      StructuredRecord.builder(schema).set("id", 100L).set("name", "Tortilla Chips").set("price", 0.99d).build(),
      StructuredRecord.builder(schema).set("id", 200L).set("name", "Water").set("price", 2.99d).build()
    );
    Assert.assertEquals(expected, emitter.getEmitted());
  }

  @Test
  public void testErrorDatasetForInvalidXml() throws Exception {
    // String field, String encoding, String xPath, String schema
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("desc", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord input = StructuredRecord.builder(inputSchema)
      .set("body",
           "<items>" +
             "<item><id>0</id><name>Burrito</name><price>7.77</price><desc>delicious</desc></item>" +
             "<item><id>100</id><name>Tortilla Chips</name><price>0.99</price></item>" +
             "<item><id>200</id><name>Water</name><price>2.99</price>" +
             "</items>")
      .build();

    XMLMultiParser parser = new XMLMultiParser(config);
    parser.initialize(new MockTransformContext("stage"));
    parser.transform(input, emitter);

    Assert.assertEquals(0, emitter.getEmitted().size());
    Assert.assertEquals(1, emitter.getErrors().size());
    InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
    Assert.assertEquals(31, invalidEntry.getErrorCode());
    Assert.assertEquals(input, invalidEntry.getInvalidRecord());
  }

  @Test
  public void testRejectsExternalEntitiesDtdAndXincludeEvenWhenConfigEnablesThem() throws Exception {
    File secretFile = TEMP_FOLDER.newFile("xmlmultiparser-secret.txt");
    Files.write(secretFile.toPath(), "SECRET_TOKEN_VALUE".getBytes(StandardCharsets.UTF_8));
    File dtdFile = TEMP_FOLDER.newFile("xmlmultiparser-external.dtd");
    Files.write(dtdFile.toPath(), "<!ENTITY extParam \"LEAKED\">".getBytes(StandardCharsets.UTF_8));

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    XMLMultiParser.Config config = new XMLMultiParser.Config(
      "body", "UTF-8", "/items/item", schema.toString(), true, true, true, false);
    Assert.assertFalse(config.shouldEnableExternalGeneralEntities());
    Assert.assertFalse(config.shouldEnableExternalParameterEntities());
    Assert.assertFalse(config.shouldLoadExternalDTD());
    Assert.assertTrue(config.shouldDisallowDocTypeDTD());

    XMLMultiParser parser = new XMLMultiParser(config);
    parser.initialize(new MockTransformContext("stage"));

    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    List<String> disallowedPayloads = ImmutableList.of(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE items [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
        + "<items><item><id>1</id><name>&xxe;</name></item></items>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE items SYSTEM \"" + dtdFile.toURI() + "\">"
        + "<items><item><id>1</id><name>Item</name></item></items>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE items [<!ENTITY % remote SYSTEM \"" + dtdFile.toURI() + "\"> %remote;]>"
        + "<items><item><id>1</id><name>Item</name></item></items>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<items><item><id>1</id><name><xi:include xmlns:xi=\"http://www.w3.org/2001/XInclude\" href=\""
        + secretFile.toURI() + "\" parse=\"text\"/></name></item></items>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<items><item><id>1</id><name><include xmlns=\"http://www.w3.org/2001/XInclude\" href=\""
        + secretFile.toURI() + "\" parse=\"text\"/></name></item></items>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<items><item><id>1</id><name><xi:include href=\""
        + secretFile.toURI() + "\" parse=\"text\"/></name></item></items>"
    );

    for (String payload : disallowedPayloads) {
      MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
      StructuredRecord input = StructuredRecord.builder(inputSchema).set("body", payload).build();
      parser.transform(input, emitter);
      Assert.assertTrue("Expected zero emitted records for payload: " + payload, emitter.getEmitted().isEmpty());
      Assert.assertEquals("Expected error record for payload: " + payload, 1, emitter.getErrors().size());
      Assert.assertEquals(31, emitter.getErrors().get(0).getErrorCode());
    }

    // Verify predefined XML entities still parse cleanly.
    MockEmitter<StructuredRecord> validEmitter = new MockEmitter<>();
    StructuredRecord validInput = StructuredRecord.builder(inputSchema)
      .set("body", "<items><item><id>1</id><name>Chips &amp; Salsa &lt;hot&gt;</name></item></items>")
      .build();
    parser.transform(validInput, validEmitter);
    Assert.assertEquals(0, validEmitter.getErrors().size());
    Assert.assertEquals(1, validEmitter.getEmitted().size());
    Assert.assertEquals("Chips & Salsa <hot>", validEmitter.getEmitted().get(0).get("name"));
  }
}
