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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class XMLMultiParserTest {

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
}
