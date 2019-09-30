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

package io.cdap.plugin.batch.aggregator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Test cases for {@link RowDenormalizerConfig}.
 */
public class RowDenormalizerConfigTest {

  @Test
  public void testDenormalizerConfig() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "NameField", "ValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");

    Assert.assertEquals(ImmutableSet.of("Firstname", "lname", "addr"), config.getOutputSchemaFields());
    Assert.assertEquals(ImmutableMap.of("Lastname", "lname", "Address", "addr"), config.getFieldAliases());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoKeyField() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("", "NameField", "ValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoFieldName() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "", "ValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoFieldValue() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "NameField", "", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");
    config.validate();
  }

  @Test(expected = ValidationException.class)
  public void testDenormalizerWithWrongKeyField() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "record",
      Schema.Field.of("KeyField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("NameField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ValueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema, Collections.emptyMap());
    RowDenormalizerConfig config = new RowDenormalizerConfig("WrongKeyField", "NameField", "ValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");
    RowDenormalizerAggregator aggregator = new RowDenormalizerAggregator(config);
    aggregator.configurePipeline(configurer);
    configurer.getStageConfigurer().getFailureCollector().getOrThrowException();
  }

  @Test(expected = ValidationException.class)
  public void testDenormalizerWithWrongNameField() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "record",
      Schema.Field.of("KeyField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("NameField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ValueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema, Collections.emptyMap());
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "WrongNameField", "ValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");
    RowDenormalizerAggregator aggregator = new RowDenormalizerAggregator(config);
    aggregator.configurePipeline(configurer);
    configurer.getStageConfigurer().getFailureCollector().getOrThrowException();
  }

  @Test(expected = ValidationException.class)
  public void testDenormalizerWithWrongValueField() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "record",
      Schema.Field.of("KeyField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("NameField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("ValueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema, Collections.emptyMap());
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "NameField", "WrongValueField", "Firstname," +
      "Lastname,Address", "Lastname:lname,Address:addr");

    RowDenormalizerAggregator aggregator = new RowDenormalizerAggregator(config);
    aggregator.configurePipeline(configurer);
    configurer.getStageConfigurer().getFailureCollector().getOrThrowException();
  }
}
