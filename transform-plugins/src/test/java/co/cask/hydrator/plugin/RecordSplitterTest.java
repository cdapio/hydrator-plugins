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
 * Tests {@link RecordSplitter}.
 */
public class RecordSplitterTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)));

  @Test
  public void testSplitter() throws Exception {
    Schema output = Schema.recordOf("output",
                                    Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
    RecordSplitter.Config config = new RecordSplitter.Config("a", "\n", "b", output.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new RecordSplitter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap\nrocks").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
    Assert.assertEquals("cdap", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("rocks", emitter.getEmitted().get(1).get("b"));
  }
}
