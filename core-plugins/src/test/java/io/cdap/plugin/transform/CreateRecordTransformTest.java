/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for {@link CreateRecordTransform}
 */
public class CreateRecordTransformTest {
  private static final Schema SAMPLE_INPUT_SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("customer_phone", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("order_id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("product_id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("amount", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("order_description", Schema.of(Schema.Type.STRING)));

  private static final Schema EXPECTED_CUSTOMER_SCHEMA =
    Schema.recordOf("customer",
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("phone", Schema.of(Schema.Type.STRING))
    );
  private static final Schema EXPECTED_ORDERS_SCHEMA =
    Schema.recordOf("orders",
                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("product_id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("amount", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("description", Schema.of(Schema.Type.STRING))
    );
  private static final Schema EXPECTED_OUTPUT_SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("customer", EXPECTED_CUSTOMER_SCHEMA),
                    Schema.Field.of("orders", EXPECTED_ORDERS_SCHEMA)

    );
  private static final Schema EXPECTED_NON_MAPPED_OUTPUT_SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("customer", EXPECTED_CUSTOMER_SCHEMA),
                    Schema.Field.of("orders", EXPECTED_ORDERS_SCHEMA),
                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))

    );
  private static final String FIELD_MAPPING = "{\"id\": [\"customer_id\"], \"customer\": {\"name\": " +
    "[\"customer_name\"],\"phone\": [\"customer_phone\"]},\"orders\": {\"id\": [\"order_id\"],\"product_id\":" +
    " [\"product_id\"],\"amount\": [\"amount\"],\"description\": [\"order_description\"]}}";

  private static final StructuredRecord SIMPLE_RECORD = StructuredRecord.builder(SAMPLE_INPUT_SCHEMA)
    .set("customer_id", 1)
    .set("customer_name", "John")
    .set("customer_phone", "Doe")
    .set("order_id", 5)
    .set("product_id", 101)
    .set("amount", 199.99)
    .set("order_description", "Crowbar")
    .build();

  private static final String STAGE = "stage";
  private static final String MOCK_STAGE = "mockstage";

  @Test
  public void testConfigurePipelineSchemaValidation() {
    MockPipelineConfigurer mockConfigurer = new MockPipelineConfigurer(SAMPLE_INPUT_SCHEMA, Collections.emptyMap());
    CreateRecordTransform.CreateRecordTransformConfig config =
      new CreateRecordTransform.CreateRecordTransformConfig(FIELD_MAPPING, "off");
    new CreateRecordTransform(config).configurePipeline(mockConfigurer);
    Assert.assertEquals(EXPECTED_OUTPUT_SCHEMA, mockConfigurer.getOutputSchema());
  }

  @Test
  public void testConfigrePipelineSchemaValidationWithNonMappedFields() {
    final String modifiedFieldMapping = FIELD_MAPPING.replace("\"id\": [\"customer_id\"],", "");
    MockPipelineConfigurer mockConfigurer = new MockPipelineConfigurer(SAMPLE_INPUT_SCHEMA, Collections.emptyMap());
    CreateRecordTransform.CreateRecordTransformConfig configNonMappedFields =
      new CreateRecordTransform.CreateRecordTransformConfig(modifiedFieldMapping, "on");
    new CreateRecordTransform(configNonMappedFields).configurePipeline(mockConfigurer);
    Assert.assertEquals(EXPECTED_NON_MAPPED_OUTPUT_SCHEMA, mockConfigurer.getOutputSchema());
  }

  @Test
  public void testConfigurePipelineSchemaValidationError() {
    MockPipelineConfigurer mockConfigurer = new MockPipelineConfigurer(SAMPLE_INPUT_SCHEMA, Collections.emptyMap());
    String badMapping = FIELD_MAPPING.replace("customer_id", "customer_i");
    CreateRecordTransform.CreateRecordTransformConfig config =
      new CreateRecordTransform.CreateRecordTransformConfig(badMapping, "off");
    try {
      new CreateRecordTransform(config).configurePipeline(mockConfigurer);
    } catch (Exception e) {
      FailureCollector collector = mockConfigurer.getStageConfigurer().getFailureCollector();
      Assert.assertEquals(1, collector.getValidationFailures().size());
      Assert.assertEquals("fieldMapping", collector.getValidationFailures().get(0).getCauses().get(0)
        .getAttribute("stageConfig"));
    }
  }
}
