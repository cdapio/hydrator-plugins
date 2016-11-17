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
package co.cask.hydrator.plugin.spark.test;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.hydrator.plugin.spark.HashingTFFeatureGenerator;
import co.cask.hydrator.plugin.spark.SkipGramFeatureGenerator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Feature Generator configs.
 */
public class FeatureGeneratorConfigTest {
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.INT)));

  @Test
  public void testInvalidInputTypeHashingTFFeatureGenerator() {
    HashingTFFeatureGenerator featureGenerator = new HashingTFFeatureGenerator(new HashingTFFeatureGenerator
      .HashingTFConfig("", 10, "offset:result"));
    try {
      featureGenerator.configurePipeline(new MockPipelineConfigurer(INPUT));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Field to be transformed should be of type String or Nullable String or Array of type " +
                            "String or Nullable String . But was INT for field offset.", e.getMessage());
    }
  }

  @Test
  public void testInvalidInputTypeSkipGramFeatureGenerator() {
    SkipGramFeatureGenerator featureGenerator = new SkipGramFeatureGenerator(new SkipGramFeatureGenerator
      .FeatureGeneratorConfig("model", "model", "body:result", ""));
    try {
      featureGenerator.configurePipeline(new MockPipelineConfigurer(INPUT));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Field to be transformed should be of type String or Nullable String or Array of type " +
                            "String or Nullable String . But was INT for field body.", e.getMessage());
    }
  }

  @Test
  public void testInvalidInputHashingTFFeatureGenerator() {
    HashingTFFeatureGenerator featureGenerator = new HashingTFFeatureGenerator(new HashingTFFeatureGenerator
      .HashingTFConfig("", 10, "text:result"));
    try {
      featureGenerator.configurePipeline(new MockPipelineConfigurer(INPUT));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Input field text does not exist in the input schema {\"type\":\"record\",\"name\":" +
                            "\"input\",\"fields\":[{\"name\":\"offset\",\"type\":\"int\"},{\"name\":\"body\"," +
                            "\"type\":\"int\"}]}.", e.getMessage());
    }
  }

  @Test
  public void testInvalidInputSkipGramTrainer() {
    HashingTFFeatureGenerator featureGenerator = new HashingTFFeatureGenerator(new HashingTFFeatureGenerator
      .HashingTFConfig("", 10, "text:result"));
    try {
      featureGenerator.configurePipeline(new MockPipelineConfigurer(INPUT));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Input field text does not exist in the input schema {\"type\":\"record\",\"name\":" +
                            "\"input\",\"fields\":[{\"name\":\"offset\",\"type\":\"int\"},{\"name\":\"body\"," +
                            "\"type\":\"int\"}]}.", e.getMessage());
    }
  }
}
