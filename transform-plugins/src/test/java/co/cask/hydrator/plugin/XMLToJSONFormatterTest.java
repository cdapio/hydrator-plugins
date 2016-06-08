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
import co.cask.cdap.etl.api.Transform;
import co.cask.hydrator.common.test.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link XMLToJSONFormatter}
 */
public class XMLToJSONFormatterTest {
  private static final Schema INPUT = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  @Test
  public void testJSONParser() throws Exception {
    XMLToJSONConverter.Config config = new XMLToJSONConverter.Config("body");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLToJSONConverter(config);
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
    Assert.assertNotNull(emitter.getEmitted().get(0).get("jsonBody"));
    Assert.assertEquals("{\"CATALOG\":" +
                          "{\"PLANT\":" +
                          "[{\"COMMON\":\"Bloodroot\",\"BOTANICAL\":\"Sanguinaria canadensis\"}," +
                          "{\"COMMON\":\"Columbine\",\"BOTANICAL\":\"Aquilegia canadensis\"}]}}",
                        emitter.getEmitted().get(0).get("jsonBody"));
  }
}
