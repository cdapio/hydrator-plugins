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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.sink.HBaseSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for hbase sink, with valid and invalid (input, output) schemas
 */
public class HBaseSinkTest {

  @Test
  public void testTableSinkWithOutputSchemaExtraField() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("points", Schema.of(Schema.Type.DOUBLE))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);

    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testTableSinkWithMissingRowKeyField() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );
    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);

    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testTableSinkWithComplexTypeSkipped() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }


  @Test
  public void testTableSinkWithComplexType() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("complex", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))
      ));

    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);

    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causes = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(2, causes.size());
  }

  @Test
  public void testTableSinkWithFieldTypeMismatch() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.INT))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );

    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);

    FailureCollector collector = mockPipelineConfigurer.getStageConfigurer().getFailureCollector();
    Assert.assertEquals(1, collector.getValidationFailures().size());
    List<ValidationFailure.Cause> causes = collector.getValidationFailures().get(0).getCauses();
    Assert.assertEquals(2, causes.size());
  }

  @Test
  public void testTableSinkWithMissingOutputSchema() {
    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", null);
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null);
    tableSink.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testTableSink() {
    Schema outputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    Schema inputSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    HBaseSink.HBaseSinkConfig tableSinkConfig = new HBaseSink.HBaseSinkConfig("hbaseSink",
                                                                              "rowkey", outputSchema.toString());
    HBaseSink tableSink = new HBaseSink(tableSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputSchema);
    tableSink.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(outputSchema, mockPipelineConfigurer.getOutputSchema());
  }
}
