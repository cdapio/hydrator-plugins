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
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class XMLMultiParserTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

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
  public void testXxeDoctypeBlocked() throws Exception {
    File secretFile = tmpFolder.newFile("multiparser-secret.txt");
    Files.write(secretFile.toPath(), "MULTIPARSER_SECRET_TOKEN".getBytes(StandardCharsets.UTF_8));

    Schema schema = Schema.recordOf(
      "record", Schema.Field.of("secret", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());

    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord input = StructuredRecord.builder(inputSchema)
      .set("body", "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE items [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
        + "<items><item><secret>&xxe;</secret></item></items>")
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    XMLMultiParser parser = new XMLMultiParser(config);
    parser.initialize(new MockTransformContext("stage"));
    parser.transform(input, emitter);

    Assert.assertTrue("XXE payload must not emit records", emitter.getEmitted().isEmpty());
    Assert.assertEquals(1, emitter.getErrors().size());
    Assert.assertEquals("Unable to parse the xml document. This record will be dropped.",
                        emitter.getErrors().get(0).getErrorMsg());
  }

  @Test
  public void testXIncludeDoesNotResolveExternalFile() throws Exception {
    File secretFile = tmpFolder.newFile("multiparser-xinclude.txt");
    Files.write(secretFile.toPath(), "MULTIPARSER_XINCLUDE_SECRET".getBytes(StandardCharsets.UTF_8));

    Schema schema = Schema.recordOf(
      "record", Schema.Field.of("secret", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());

    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord input = StructuredRecord.builder(inputSchema)
      .set("body", "<?xml version=\"1.0\"?>"
        + "<items xmlns:xi=\"http://www.w3.org/2001/XInclude\">"
        + "<item><secret><xi:include href=\"" + secretFile.toURI() + "\" parse=\"text\"/></secret></item>"
        + "</items>")
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    XMLMultiParser parser = new XMLMultiParser(config);
    parser.initialize(new MockTransformContext("stage"));
    parser.transform(input, emitter);

    Assert.assertEquals(1, emitter.getEmitted().size());
    String value = emitter.getEmitted().get(0).get("secret");
    Assert.assertFalse("XInclude must not resolve external files",
                       value != null && value.contains("MULTIPARSER_XINCLUDE_SECRET"));
  }

  @Test
  public void testRejectsUnsafeLegacySecurityProperties() throws Exception {
    Schema schema = Schema.recordOf("record", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());
    setPluginProperties(config, ImmutableMap.of(
      XmlSecurityUtils.ENABLE_EXTERNAL_GENERAL_ENTITIES, "true",
      XmlSecurityUtils.ENABLE_EXTERNAL_PARAMETER_ENTITIES, "true",
      XmlSecurityUtils.LOAD_EXTERNAL_DTD, "true",
      XmlSecurityUtils.DISALLOW_DOCTYPE_DTD, "false"
    ));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    new XMLMultiParser(config).configurePipeline(configurer);
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
      new XMLMultiParser(config).initialize(new MockTransformContext("stage"));
      Assert.fail("Expected ValidationException in initialize for unsafe legacy XML properties");
    } catch (ValidationException e) {
      Assert.assertEquals(4, e.getFailures().size());
    }

    try {
      new XMLMultiParser(config).prepareRun(createMockStageSubmitterContext());
      Assert.fail("Expected ValidationException in prepareRun for unsafe legacy XML properties");
    } catch (ValidationException e) {
      Assert.assertEquals(4, e.getFailures().size());
    }
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
    Schema schema = Schema.recordOf("record", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Schema inputSchema = Schema.recordOf("input", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    XMLMultiParser.Config config = new XMLMultiParser.Config("body", "UTF-8", "/items/item", schema.toString());
    setPluginProperties(config, ImmutableMap.of(
      XmlSecurityUtils.ENABLE_EXTERNAL_GENERAL_ENTITIES, "false",
      XmlSecurityUtils.ENABLE_EXTERNAL_PARAMETER_ENTITIES, "false",
      XmlSecurityUtils.LOAD_EXTERNAL_DTD, "false",
      XmlSecurityUtils.DISALLOW_DOCTYPE_DTD, "true"
    ));

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    XMLMultiParser parser = new XMLMultiParser(config);
    parser.configurePipeline(configurer);
    Assert.assertTrue(configurer.getStageConfigurer().getFailureCollector().getValidationFailures().isEmpty());

    parser.initialize(new MockTransformContext("stage"));
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    parser.transform(
      StructuredRecord.builder(inputSchema).set("body", "<items><item><name>Burrito</name></item></items>").build(),
      emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals("Burrito", emitter.getEmitted().get(0).get("name"));
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
