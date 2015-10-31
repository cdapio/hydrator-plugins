/*
 * Copyright © 2015 Cask Data, Inc.
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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link SentimentDetector}.
 */
public class SentimentDetectorTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("text", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                       Schema.Field.of("tag", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("weight", Schema.of(Schema.Type.DOUBLE)));
  
  @Test
  public void testAutoTagger() throws Exception {
    SentimentDetector.Config config = new SentimentDetector.Config("XYZ",
                                                     "text", "tag", "weight", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new SentimentDetector(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("id", 1)
                          .set("text", "I love Alice. I hate Bob.")
                          .build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
  }
}
