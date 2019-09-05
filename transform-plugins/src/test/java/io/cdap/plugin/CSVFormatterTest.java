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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link CSVFormatter}.
 */
public class CSVFormatterTest {

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));


  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  @Test
  public void testDefaultCSVFormatter() throws Exception {
    CSVFormatter.Config config = new CSVFormatter.Config("DELIMITED", "VBAR", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVFormatter(config);
    transform.initialize(new MockTransformContext());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    // Test missing field.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("a", "1")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    Assert.assertEquals("1|2|3|4|5\r\n", emitter.getEmitted().get(0).get("body"));
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("a", "6")
                          .set("b", "7")
                          .set("c", "8")
                          .set("d", "9")
                          .set("e", "10").build(), emitter);
    Assert.assertEquals("6|7|8|9|10\r\n", emitter.getEmitted().get(0).get("body"));
  }

  @Test
  public void testSchemaValidation() {
    CSVFormatter.Config config = new CSVFormatter.Config("DELIMITED", "VBAR", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVFormatter(config);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT, mockPipelineConfigurer.getOutputSchema());
  }

  @Test
  public void testMultipleInvalidConfigProperties() {
    CSVFormatter.Config config = new CSVFormatter.Config("", "INVALIDDELIM", OUTPUT.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new CSVFormatter(config);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT1);
    // Error message here reflects information defined in the CSVFormatter.Config class and CSVFormatter's delimMap.
    // Any change to the error message presented in CSVFormatter.Config should be reflected here, and vice versa.
    final List<ValidationFailure> expectedFailures = ImmutableList.of(
        new ValidationFailure("Unknown delimiter 'INVALIDDELIM' specified.",
            "Please specify one of the following: "
                + "COMMA, CTRL-A, CARET, HASH, TAB, STAR, VBAR, CTRL-D, CTRL-E, DOLLAR, TILDE, CTRL-B, CTRL-C, CTRL-F")
            .withConfigProperty("delimiter"),
        new ValidationFailure("Format is not specified.",
            "Please specify one of the following: DELIMITED, EXCEL, MYSQL, RFC4180 & TDF")
            .withConfigProperty("format"),
        new ValidationFailure("Format specified is not one of the allowed values.",
            "Please specify one of the following: DELIMITED, EXCEL, MYSQL, RFC4180 & TDF")
            .withConfigProperty("format"));

    transform.configurePipeline(mockPipelineConfigurer);
    MockFailureCollector failureCollector
        = (MockFailureCollector) mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(expectedFailures, failureCollector.getValidationFailures());
  }
}
