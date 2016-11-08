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
import co.cask.hydrator.common.MockPipelineConfigurer;
import co.cask.hydrator.common.test.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link FastFilter}.
 */
public class FastFilterTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)));

  @Test
  public void testFastFilterEquals() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "=", "cdap", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cask").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterGreaterThan() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", ">", "m", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "a").build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "z").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "m").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterGreaterThanEqual() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", ">=", "m", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "a").build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "z").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "m").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterLessThan() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "<", "m", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "a").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "z").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "m").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterLessThanEqual() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "<=", "m", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "a").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "z").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "m").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterContains() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "contains", "cdap", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is a cdap test").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is not").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is also not one").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterDoesNotContain() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "does not contain", "cdap", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is a cdap test").build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is not").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is also not one").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterStartsWith() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "starts with", "cdap", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is the first").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is not the last cdap").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "maybe cdap is in the middle").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterEndsWith() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "ends with", "cdap", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is the first").build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "this is not the last cdap").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "maybe cdap is in the middle").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterMatchesRegex() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "matches regex", "\\d", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is the first").build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is number 1").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "1 is the number cdap is").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterDoesNotMatchRegex() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "does not match regex", "\\d", false);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is the first").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "cdap is number 1").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "1 is the number cdap is").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
  }

  @Test
  public void testFastFilterIgnoreCase() throws Exception {
    FastFilter.Config config = new FastFilter.Config("a", "contains", "cdap", true);
    Transform<StructuredRecord, StructuredRecord> transform = new FastFilter(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT).set("a", "CDAP is the first").build(), emitter);
    Assert.assertEquals(1, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "CDAP is number 1").build(), emitter);
    Assert.assertEquals(2, emitter.getEmitted().size());
    transform.transform(StructuredRecord.builder(INPUT).set("a", "1 is the number CDAP is").build(), emitter);
    Assert.assertEquals(3, emitter.getEmitted().size());
  }
}
