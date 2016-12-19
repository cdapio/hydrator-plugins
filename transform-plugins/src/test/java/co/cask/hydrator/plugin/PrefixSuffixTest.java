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

import java.text.SimpleDateFormat;

/**
 * Tests {@link PrefixSuffix}.
 */
public class PrefixSuffixTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)));

  @Test
  public void testPrefix() throws Exception {
    Schema output = Schema.recordOf("output",
                                    Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    PrefixSuffix.Config config = new PrefixSuffix.Config("a\tcask_", null, output.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new PrefixSuffix(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap").build(), emitter);
    Assert.assertEquals("cask_cdap", emitter.getEmitted().get(0).get("a"));
  }

  @Test
  public void testSuffix() throws Exception {
    Schema output = Schema.recordOf("output",
                                    Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    PrefixSuffix.Config config = new PrefixSuffix.Config(null, "a\t_rocks", output.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new PrefixSuffix(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap").build(), emitter);
    Assert.assertEquals("cdap_rocks", emitter.getEmitted().get(0).get("a"));
  }

  @Test
  public void testPrefixSuffix() throws Exception {
    Schema output = Schema.recordOf("output",
                                    Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    PrefixSuffix.Config config = new PrefixSuffix.Config("a\tcask_", "a\t_rocks", output.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new PrefixSuffix(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap").build(), emitter);
    Assert.assertEquals("cask_cdap_rocks", emitter.getEmitted().get(0).get("a"));
  }
}
