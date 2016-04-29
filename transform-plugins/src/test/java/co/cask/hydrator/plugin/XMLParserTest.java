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
 * Created by nmotgi on 4/17/16.
 */
public class XmlParserTest {

  private static final Schema INPUT1 = Schema.recordOf("output",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));


  private static final Schema OUTPUT = Schema.recordOf("input1",
                                                       Schema.Field.of("title", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("author", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("year", Schema.of(Schema.Type.STRING)));

  String xml = "";

  @Test
  public void testDefaultCSVFormatter() throws Exception {
    XmlParser.Config config = new XmlParser.Config("body", "title:/book/title,author:/book/author,year:/book/year",
                                                   OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new XmlParser(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title> " +
                            "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>")
                          .build(), emitter);

    Assert.assertEquals(emitter.getEmitted().size(), 1);
  }
}
