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
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Encoder}
 */
public class EncoderTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.BYTES)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUTSTR = Schema.recordOf("outputstr",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

  @Test
  public void testBase64Encoder() throws Exception {
    String test = "This is a test for testing base64 encoding";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:BASE64", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", test)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    Base64 base64 = new Base64();
    byte[] expected = base64.encode(test.getBytes("UTF-8"));
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testBase64StringEncoder() throws Exception {
    String test = "This is a test for testing base64 encoding";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:STRING_BASE64", OUTPUTSTR.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", test)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    Base64 base64 = new Base64();
    String expected = base64.encodeAsString(test.getBytes("UTF-8"));
    String actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testBase32Encoder() throws Exception {
    String test = "This is a test for testing base32 encoding";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:BASE32", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", test)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    Base32 base32 = new Base32();
    byte[] expected = base32.encode(test.getBytes("UTF-8"));
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testStringBase32Encoder() throws Exception {
    String test = "This is a test for testing base32 encoding";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:BASE32", OUTPUTSTR.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", test)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    Base32 base32 = new Base32();
    String expected = base32.encodeAsString(test.getBytes("UTF-8"));
    String actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testHEXEncoder() throws Exception {
    String test = "This is a test for testing hex encoding";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:HEX", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", test)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    Hex hex = new Hex();
    byte[] expected = hex.encode(test.getBytes("UTF-8"));
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:BASE32", OUTPUTSTR.toString()));
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUTSTR, mockPipelineConfigurer.getOutputSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSchemaValidationWithInvalidInputSchema() throws Exception {

    Transform<StructuredRecord, StructuredRecord> transform =
      new Encoder(new Encoder.Config("a:BASE32", OUTPUTSTR.toString()));
    final Schema invalidInput = Schema.recordOf("input",
                                                Schema.Field.of("a", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("c", Schema.of(Schema.Type.STRING)));

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(invalidInput);
    transform.configurePipeline(mockPipelineConfigurer);
  }
}
