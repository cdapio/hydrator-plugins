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
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class XMLParserTest {
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Logger LOG = LoggerFactory.getLogger(XMLParserTest.class);

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

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
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute("stage", "mockstage");
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLParser.Config.FIELD_TYPE_MAPPING);
      expectedCause.addAttribute(CauseAttributes.CONFIG_ELEMENT, "category:");
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
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

  @Test
  public void testInputFieldNotInSchema() throws Exception {
    Schema schema = Schema.recordOf("record",
        Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("author", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("year", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    XMLParser.Config config = new XMLParser.Config(
        "x", "UTF-8",
        "title:/book/title,author:/book/author,year:/book/year",
        "title:string,author:string,year:string",
        "Write to error dataset");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    new XMLParser(config).configurePipeline(configurer);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLParser.Config.INPUT);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testXxeDoctypeBlocked() throws Exception {
    File secretFile = tmpFolder.newFile("xmlparser-secret.txt");
    Files.write(secretFile.toPath(), "SENSITIVE_SECRET_TOKEN".getBytes(StandardCharsets.UTF_8));

    String xxeXml = "<?xml version=\"1.0\"?>"
      + "<!DOCTYPE record [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
      + "<record><secret>&xxe;</secret></record>";

    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8", "secret:/record/secret", "secret:string", "Write to error dataset");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", xxeXml)
      .build();
    transform.transform(inputRecord, emitter);

    Assert.assertTrue("XXE payload must not emit records", emitter.getEmitted().isEmpty());
    Assert.assertEquals(1, emitter.getErrors().size());
    String errorMsg = emitter.getErrors().get(0).getErrorMsg();
    Assert.assertTrue("Expected DOCTYPE rejection error, got: " + errorMsg, errorMsg.contains("DOCTYPE"));
    Assert.assertFalse(errorMsg.contains("SENSITIVE_SECRET_TOKEN"));
  }

  @Test
  public void testXIncludeDoesNotResolveExternalFile() throws Exception {
    File secretFile = tmpFolder.newFile("xmlparser-xinclude.txt");
    Files.write(secretFile.toPath(), "XINCLUDE_LEAKED_TOKEN".getBytes(StandardCharsets.UTF_8));

    String xml = "<?xml version=\"1.0\"?>"
      + "<record xmlns:xi=\"http://www.w3.org/2001/XInclude\">"
      + "<secret><xi:include href=\"" + secretFile.toURI() + "\" parse=\"text\"/></secret>"
      + "</record>";

    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8", "secret:/record/secret", "secret:string", "Exit on error");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", xml)
      .build();
    transform.transform(inputRecord, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    String value = emitter.getEmitted().get(0).get("secret");
    Assert.assertFalse("XInclude must not resolve external files",
                       value != null && value.contains("XINCLUDE_LEAKED_TOKEN"));
  }

  @Test
  public void testRejectsUnsafeLegacySecurityProperties() throws Exception {
    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8", "title:/book/title", "title:string", "Exit on error");
    setPluginProperties(config, ImmutableMap.of(
      XmlSecurityUtils.ENABLE_EXTERNAL_GENERAL_ENTITIES, "true",
      XmlSecurityUtils.ENABLE_EXTERNAL_PARAMETER_ENTITIES, "true",
      XmlSecurityUtils.LOAD_EXTERNAL_DTD, "${loadDtd}",
      XmlSecurityUtils.DISALLOW_DOCTYPE_DTD, "false"
    ));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    new XMLParser(config).configurePipeline(configurer);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(4, failures.size());
    Set<String> failingProps = failures.stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toSet());
    Assert.assertTrue(failingProps.contains(XmlSecurityUtils.ENABLE_EXTERNAL_GENERAL_ENTITIES));
    Assert.assertTrue(failingProps.contains(XmlSecurityUtils.ENABLE_EXTERNAL_PARAMETER_ENTITIES));
    Assert.assertTrue(failingProps.contains(XmlSecurityUtils.LOAD_EXTERNAL_DTD));
    Assert.assertTrue(failingProps.contains(XmlSecurityUtils.DISALLOW_DOCTYPE_DTD));

    try {
      new XMLParser(config).initialize(new MockTransformContext());
      Assert.fail("Expected ValidationException in initialize for unsafe legacy XML properties");
    } catch (ValidationException e) {
      Assert.assertEquals(4, e.getFailures().size());
    }

    try {
      new XMLParser(config).prepareRun(createMockStageSubmitterContext());
      Assert.fail("Expected ValidationException in prepareRun for unsafe legacy XML properties");
    } catch (ValidationException e) {
      Assert.assertEquals(4, e.getFailures().size());
    }
  }

  @Test
  public void testNullFailOnArrayDefaultsToFalse() throws Exception {
    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8", "item:/items/item", "item:string", "Exit on error");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", "<items><item>first</item><item>second</item></items>")
      .build();
    transform.transform(inputRecord, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals("first", emitter.getEmitted().get(0).get("item"));
  }

  private static StageSubmitterContext createMockStageSubmitterContext() {
    FailureCollector collector = new MockFailureCollector();
    return (StageSubmitterContext) Proxy.newProxyInstance(
      StageSubmitterContext.class.getClassLoader(),
      new Class<?>[]{StageSubmitterContext.class},
      (proxy, method, args) -> "getFailureCollector".equals(method.getName()) ? collector : null);
  }

  @Test
  public void testAcceptsSafeLegacySecurityProperties() throws Exception {
    XMLParser.Config config = new XMLParser.Config(
      "body", "UTF-8", "title:/book/title", "title:string", "Exit on error");
    setPluginProperties(config, ImmutableMap.of(
      XmlSecurityUtils.ENABLE_EXTERNAL_GENERAL_ENTITIES, "false",
      XmlSecurityUtils.ENABLE_EXTERNAL_PARAMETER_ENTITIES, "false",
      XmlSecurityUtils.LOAD_EXTERNAL_DTD, "false",
      XmlSecurityUtils.DISALLOW_DOCTYPE_DTD, "true"
    ));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    XMLParser parser = new XMLParser(config);
    parser.configurePipeline(configurer);
    Assert.assertTrue(configurer.getStageConfigurer().getFailureCollector().getValidationFailures().isEmpty());

    parser.initialize(new MockTransformContext());
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    parser.transform(
      StructuredRecord.builder(INPUT).set("offset", 1).set("body", "<book><title>A &amp; B</title></book>").build(),
      emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals("A & B", emitter.getEmitted().get(0).get("title"));
  }

  private static void setPluginProperties(PluginConfig config, Map<String, String> props) throws Exception {
    PluginProperties pluginProperties = PluginProperties.builder().addAll(props).build();
    Field propertiesField = PluginConfig.class.getDeclaredField("properties");
    propertiesField.setAccessible(true);
    propertiesField.set(config, pluginProperties);
    Field rawPropertiesField = PluginConfig.class.getDeclaredField("rawProperties");
    rawPropertiesField.setAccessible(true);
    rawPropertiesField.set(config, pluginProperties);
  }
}
