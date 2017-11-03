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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class XMLParserTest {
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Logger LOG = LoggerFactory.getLogger(XMLParserTest.class);

  @Test
  public void testInvalidConfig() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "category://book/@category,title://book/title," +
      "year:/bookstore/book[price>35.00]/year,price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory",
                                                   "category:,title:string,price:double,year:int,subcategory:string",
                                                   "Exit on error");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    try {
      new XMLParser(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Type cannot be null. Please specify type for category", e.getMessage());
    }
  }

  @Test
  public void testXMLParserWithSimpleXPath() throws Exception {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("author", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("year", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8",
      "title:/book/title,author:/book/author,year:/book/year",
      "title:string,author:string,year:string",
      "Write to error dataset");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    StructuredRecord inputRecord =
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build();
    transform.transform(inputRecord, emitter);

    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("title", "Everyday Italian")
        .set("author", "Giada De Laurentiis")
        .set("year", "2005").build());
    Assert.assertEquals(expected, emitter.getEmitted());
  }

  @Test
  public void testXpathWithMultipleElements() throws Exception {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("category", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("price", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("year", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("subcategory", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-16 (Unicode with byte-order mark)",
      "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory",
      "category:string,title:string,price:double,year:int,subcategory:string",
      "Write to error dataset");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type></subcategory>" +
        "<title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author><year>2005</year>" +
        "<price>30.00</price></book></bookstore>").build();
    transform.transform(inputRecord, emitter);
    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(schema).set("category", "cooking")
        .set("title", "Everyday Italian")
        .set("subcategory", "<subcategory><type>Continental</type></subcategory>").build());
    Assert.assertEquals(expected, emitter.getEmitted());
    emitter.clear();

    inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 2)
      .set("body", "<bookstore><book category=\"children\"><subcategory><type>Series</type></subcategory>" +
        "<title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>49.99</price>" +
        "</book></bookstore>").build();
    transform.transform(inputRecord, emitter);
    expected = ImmutableList.of(
      StructuredRecord.builder(schema).set("category", "children")
        .set("title", "Harry Potter")
        .set("price", 49.99d)
        .set("year", 2005)
        .set("subcategory", "<subcategory><type>Series</type></subcategory>").build());
    Assert.assertEquals(expected, emitter.getEmitted());
    emitter.clear();

    inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 3)
      .set("body", "<bookstore><book category=\"web\"><subcategory><type>Basics</type></subcategory>" +
        "<title lang=\"en\">Learning XML</title><author>Erik T. Ray</author><year>2003</year><price>39.95</price>" +
        "</book></bookstore>").build();
    transform.transform(inputRecord, emitter);
    expected = ImmutableList.of(
      StructuredRecord.builder(schema).set("category", "web")
        .set("title", "Learning XML")
        .set("price", 39.95d)
        .set("year", 2003)
        .set("subcategory", "<subcategory><type>Basics</type></subcategory>").build());
    Assert.assertEquals(expected, emitter.getEmitted());
    emitter.clear();
  }

  @Test
  public void testEmitErrors() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:int,year:int", "Write to error dataset");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());

    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
        "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(inputRecord, emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    Assert.assertEquals(1, emitter.getErrors().size());

    InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
    Assert.assertEquals(31, invalidEntry.getErrorCode());
    Assert.assertEquals("offset", 1, invalidEntry.getInvalidRecord().<Integer>get("offset").intValue());
    Assert.assertEquals("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title><author>Giada " +
      "De Laurentiis</author><year>2005</year><price>30.00</price>" +
      "</book>", invalidEntry.getInvalidRecord().get("body"));
  }

  @Test
  public void testIgnoreError() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "ISO-8859-1", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:string,year:int", "Ignore error and continue");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>null</year><price>30.00</price></book>").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "<book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
          "<author>J K. Rowling</author><year>2005</year><price>49.99</price></book>").build());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    for (StructuredRecord record : input) {
      transform.transform(record, emitter);
    }
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(0, emitter.getErrors().size());
    StructuredRecord record = emitter.getEmitted().get(0);
    Assert.assertEquals("Harry Potter", record.get("title"));
    Assert.assertEquals("J K. Rowling", record.get("author"));
    Assert.assertEquals(2005, record.<Integer>get("year").intValue());
  }

  @Test
  public void testExitOnError() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:int,year:int", "Exit on error");
    try {
      Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
      transform.initialize(new MockTransformContext());

      StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build();

      MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
      transform.transform(inputRecord, emitter);
      Assert.fail();
    } catch (IllegalStateException e) {
      LOG.error("Test passes. Exiting on exception.", e);
    }
  }

  @Test
  public void testXpathArray() throws Exception {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("category", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8",
      "category://book/@category,title://book/title",
      "category:string,title:string",
      "Exit on error");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    StructuredRecord inputRecord =
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>" +
          "<book category=\"children\"><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author>" +
          "<year>2005</year><price>29.99</price></book></bookstore>").build();
    transform.transform(inputRecord, emitter);
    List<StructuredRecord> expected = ImmutableList.of(
      StructuredRecord.builder(schema).set("category", "cooking").set("title", "Everyday Italian").build());
    Assert.assertEquals(expected, emitter.getEmitted());
  }
}
