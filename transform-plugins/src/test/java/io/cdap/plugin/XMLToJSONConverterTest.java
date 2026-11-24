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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link XMLToJSON}
 */
public class XMLToJSONConverterTest {
  private static final Schema INPUT = Schema.recordOf("input1",
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT = Schema.recordOf("output1",
                                                      Schema.Field.of("jsonevent", Schema.of(Schema.Type.STRING)));

  @Test
  public void testJSONParser() throws Exception {
    XMLToJSON.Config config = new XMLToJSON.Config("body", "jsonevent", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new XMLToJSON(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("body",
                               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                 "<CATALOG>" +
                                 "  <PLANT>" +
                                 "    <COMMON>Bloodroot</COMMON>" +
                                 "    <BOTANICAL>Sanguinaria canadensis</BOTANICAL>" +
                                 "  </PLANT>" +
                                 "  <PLANT>" +
                                 "    <COMMON>Columbine</COMMON>" +
                                 "    <BOTANICAL>Aquilegia canadensis</BOTANICAL>" +
                                 "  </PLANT>" +
                                 "</CATALOG>")
                          .build(), emitter);
    Assert.assertNotNull(emitter.getEmitted().get(0).get("jsonevent"));
    Assert.assertEquals("{\"CATALOG\":" +
                          "{\"PLANT\":" +
                          "[{\"COMMON\":\"Bloodroot\",\"BOTANICAL\":\"Sanguinaria canadensis\"}," +
                          "{\"COMMON\":\"Columbine\",\"BOTANICAL\":\"Aquilegia canadensis\"}]}}",
                        emitter.getEmitted().get(0).get("jsonevent"));
  }

  @Test
  public void testFailure() throws Exception {
    XMLToJSON.Config config = new XMLToJSON.Config("body", "jsonevent", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new XMLToJSON(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    try {
      transform.transform(StructuredRecord.builder(INPUT)
                            .set("body",
                                 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                   "<CATALOG>" +
                                   "  <PLANT>" +
                                   "    <COMMON>Bloodroot</COMMON>" +
                                   "    <BOTANICAL>Sanguinaria canadensis</BOTANICAL>" +
                                   "  </PLANT>" +
                                   "  <PLANT>" +
                                   "    <COMMON>Columbine</COMMON>" +
                                   "    <BOTANICAL>Aquilegia canadensis</BOTANICAL>" +
                                   //"  </PLANT>" +
                                   "</CATALOG>")
                            .build(), emitter);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Failed to convert XML to JSON"));
    }
  }

  @Test
  public void testInvalidInputField() throws Exception {
    XMLToJSON.Config config = new XMLToJSON.Config("does_not_exist", "jsonevent", OUTPUT.toString());
    PipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    XMLToJSON converter = new XMLToJSON(config);

    converter.configurePipeline(configurer);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, "inputField");
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testInvalidInputFieldType() throws Exception {
    Schema schema = Schema.recordOf("input1",
        Schema.Field.of("body", Schema.of(Schema.Type.INT)));
    XMLToJSON.Config config = new XMLToJSON.Config("body", "jsonevent", OUTPUT.toString());
    PipelineConfigurer configurer = new MockPipelineConfigurer(schema);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    XMLToJSON converter = new XMLToJSON(config);

    converter.configurePipeline(configurer);

    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(2, collector.getValidationFailures().get(0).getCauses().size());
  }

  @Test
  public void testRejectsDoctypeExternalEntitiesAndXinclude() throws Exception {
    XMLToJSON.Config config = new XMLToJSON.Config("body", "jsonevent", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new XMLToJSON(config);
    transform.initialize(null);

    String[] disallowedPayloads = new String[] {
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE catalog SYSTEM \"file:///etc/passwd\">"
        + "<catalog>test</catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE catalog [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
        + "<catalog>&xxe;</catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE catalog [<!ENTITY % remote SYSTEM \"file:///etc/passwd\"> %remote;]>"
        + "<catalog>test</catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!ENTITY standalone SYSTEM \"file:///etc/passwd\">"
        + "<catalog>test</catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<catalog><xi:include href=\"file:///etc/passwd\" parse=\"text\"/></catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<catalog><include xmlns=\"http://www.w3.org/2001/XInclude\" href=\"file:///etc/passwd\"/></catalog>"
    };

    for (String payload : disallowedPayloads) {
      MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
      try {
        transform.transform(StructuredRecord.builder(INPUT).set("body", payload).build(), emitter);
        Assert.fail("Expected exception for disallowed XML payload: " + payload);
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("Failed to convert XML to JSON"));
        Assert.assertNotNull(e.getCause());
        Assert.assertTrue(
          e.getCause().getMessage().contains("DOCTYPE declarations, external entities, and XInclude are not allowed"));
      }
      Assert.assertTrue(emitter.getEmitted().isEmpty());
    }

    // Verify predefined XML entities still convert cleanly.
    MockEmitter<StructuredRecord> validEmitter = new MockEmitter<>();
    transform.transform(
      StructuredRecord.builder(INPUT).set("body", "<catalog><name>A &amp; B &lt;C&gt;</name></catalog>").build(),
      validEmitter);
    Assert.assertEquals(1, validEmitter.getEmitted().size());
    Assert.assertEquals("{\"catalog\":{\"name\":\"A & B <C>\"}}", validEmitter.getEmitted().get(0).get("jsonevent"));
  }
}
