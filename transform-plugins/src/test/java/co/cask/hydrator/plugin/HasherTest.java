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
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Hasher}
 */
public class HasherTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  @Test
  public void testHasherMD2() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("MD2", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.md2Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.md2Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.md2Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testHasherMD5() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("MD5", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.md5Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.md5Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.md5Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testHasherSHA1() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("SHA1", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.sha1Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.sha1Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.sha1Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testHasherSHA256() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("SHA256", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.sha256Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.sha256Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.sha256Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testHasherSHA384() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("SHA384", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.sha384Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.sha384Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.sha384Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testHasherSHA512() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("SHA512", "a,b,e"));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "Field A")
                          .set("b", "Field B")
                          .set("c", "Field C")
                          .set("d", 4)
                          .set("e", "Field E").build(), emitter);

    ;
    Assert.assertEquals(5, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(DigestUtils.sha512Hex("Field A"), emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals(DigestUtils.sha512Hex("Field B"), emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("Field C", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(DigestUtils.sha512Hex("Field E"), emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Hasher(new Hasher.Config("SHA512", "a,b,e"));
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(INPUT, mockPipelineConfigurer.getOutputSchema());
  }
}
