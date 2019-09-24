/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link CloneRecord}.
 */
public class CloneRecordTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  @Test
  public void testCloneRecord() throws Exception {
    CloneRecord.Config config = new CloneRecord.Config(5);
    Transform<StructuredRecord, StructuredRecord> transform = new CloneRecord(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "1")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    Assert.assertEquals(5, emitter.getEmitted().size());
  }

  @Test
  public void testSchemaValidation() {
    CloneRecord.Config config = new CloneRecord.Config(5);
    Transform<StructuredRecord, StructuredRecord> transform = new CloneRecord(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);

    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(INPUT, mockPipelineConfigurer.getOutputSchema());
  }

  @Test
  public void testInvalidNumberOfCopies() {
    CloneRecord.Config config = new CloneRecord.Config(0);
    Transform<StructuredRecord, StructuredRecord> transform = new CloneRecord(config);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);
    transform.configurePipeline(mockPipelineConfigurer);
    MockFailureCollector failureCollector
      = (MockFailureCollector) mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
  }
}
