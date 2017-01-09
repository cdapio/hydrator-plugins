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
 * Tests {@link Decoder}
 */
public class DecoderTest {
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
  public void testBase64Decoder() throws Exception {
    String test = "This is a test for testing base64 decoding";
    Transform<StructuredRecord, StructuredRecord> encoder =
      new Encoder(new Encoder.Config("a:BASE64", OUTPUT.toString()));
    encoder.initialize(null);

    MockEmitter<StructuredRecord> emitterEncoded = new MockEmitter<>();
    encoder.transform(StructuredRecord.builder(INPUT)
                        .set("a", test)
                        .set("b", "2")
                        .set("c", "3")
                        .set("d", "4")
                        .set("e", "5").build(), emitterEncoded);

    Base64 base64 = new Base64();
    byte[] expected = base64.encode(test.getBytes("UTF-8"));
    byte[] actual = emitterEncoded.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitterEncoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);

    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:BASE64", OUTPUTSTR.toString()));
    decoder.initialize(null);
    MockEmitter<StructuredRecord> emitterDecoded = new MockEmitter<>();
    decoder.transform(emitterEncoded.getEmitted().get(0), emitterDecoded);
    Assert.assertEquals(2, emitterDecoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(test, emitterDecoded.getEmitted().get(0).get("a"));
  }

  @Test
  public void testBase64AsStringDecoder() throws Exception {
    String test = "This is a test for testing string base64 decoding";
    Transform<StructuredRecord, StructuredRecord> encoder =
      new Encoder(new Encoder.Config("a:STRING_BASE64", OUTPUT.toString()));
    encoder.initialize(null);

    MockEmitter<StructuredRecord> emitterEncoded = new MockEmitter<>();
    encoder.transform(StructuredRecord.builder(INPUT)
                        .set("a", test)
                        .set("b", "2")
                        .set("c", "3")
                        .set("d", "4")
                        .set("e", "5").build(), emitterEncoded);

    Base64 base64 = new Base64();
    byte[] expected = base64.encode(test.getBytes("UTF-8"));
    byte[] actual = emitterEncoded.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitterEncoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);

    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:STRING_BASE64", OUTPUTSTR.toString()));
    decoder.initialize(null);
    MockEmitter<StructuredRecord> emitterDecoded = new MockEmitter<>();
    decoder.transform(emitterEncoded.getEmitted().get(0), emitterDecoded);
    Assert.assertEquals(2, emitterDecoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(test, emitterDecoded.getEmitted().get(0).get("a"));
  }

  @Test
  public void testBase32Decoder() throws Exception {
    String test = "This is a test for testing base32 decoding";
    Transform<StructuredRecord, StructuredRecord> encoder =
      new Encoder(new Encoder.Config("a:BASE32", OUTPUT.toString()));
    encoder.initialize(null);

    MockEmitter<StructuredRecord> emitterEncoded = new MockEmitter<>();
    encoder.transform(StructuredRecord.builder(INPUT)
                        .set("a", test)
                        .set("b", "2")
                        .set("c", "3")
                        .set("d", "4")
                        .set("e", "5").build(), emitterEncoded);

    Base32 base32 = new Base32();
    byte[] expected = base32.encode(test.getBytes("UTF-8"));
    byte[] actual = emitterEncoded.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitterEncoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);

    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:BASE32", OUTPUTSTR.toString()));
    decoder.initialize(null);
    MockEmitter<StructuredRecord> emitterDecoded = new MockEmitter<>();
    decoder.transform(emitterEncoded.getEmitted().get(0), emitterDecoded);
    Assert.assertEquals(2, emitterDecoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(test, emitterDecoded.getEmitted().get(0).get("a"));
  }

  @Test
  public void testBase32AsStringDecoder() throws Exception {
    String test = "This is a test for testing string base32 decoding";
    Transform<StructuredRecord, StructuredRecord> encoder =
      new Encoder(new Encoder.Config("a:STRING_BASE32", OUTPUT.toString()));
    encoder.initialize(null);

    MockEmitter<StructuredRecord> emitterEncoded = new MockEmitter<>();
    encoder.transform(StructuredRecord.builder(INPUT)
                        .set("a", test)
                        .set("b", "2")
                        .set("c", "3")
                        .set("d", "4")
                        .set("e", "5").build(), emitterEncoded);

    Base32 base32 = new Base32();
    byte[] expected = base32.encode(test.getBytes("UTF-8"));
    byte[] actual = emitterEncoded.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitterEncoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);

    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:STRING_BASE32", OUTPUTSTR.toString()));
    decoder.initialize(null);
    MockEmitter<StructuredRecord> emitterDecoded = new MockEmitter<>();
    decoder.transform(emitterEncoded.getEmitted().get(0), emitterDecoded);
    Assert.assertEquals(2, emitterDecoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(test, emitterDecoded.getEmitted().get(0).get("a"));
  }

  @Test
  public void testHexDecoder() throws Exception {
    String test = "This is a test for testing hex decoding";
    Transform<StructuredRecord, StructuredRecord> encoder =
      new Encoder(new Encoder.Config("a:HEX", OUTPUT.toString()));
    encoder.initialize(null);

    MockEmitter<StructuredRecord> emitterEncoded = new MockEmitter<>();
    encoder.transform(StructuredRecord.builder(INPUT)
                        .set("a", test)
                        .set("b", "2")
                        .set("c", "3")
                        .set("d", "4")
                        .set("e", "5").build(), emitterEncoded);

    Hex hex = new Hex();
    byte[] expected = hex.encode(test.getBytes("UTF-8"));
    byte[] actual = emitterEncoded.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitterEncoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);

    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:HEX", OUTPUTSTR.toString()));
    decoder.initialize(null);
    MockEmitter<StructuredRecord> emitterDecoded = new MockEmitter<>();
    decoder.transform(emitterEncoded.getEmitted().get(0), emitterDecoded);
    Assert.assertEquals(2, emitterDecoded.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(test, emitterDecoded.getEmitted().get(0).get("a"));
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:BASE64", OUTPUT.toString()));
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);
    decoder.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT, mockPipelineConfigurer.getOutputSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSchemaValidationWithInvalidInputSchema() throws Exception {
    Transform<StructuredRecord, StructuredRecord> decoder =
      new Decoder(new Decoder.Config("a:BASE64", OUTPUT.toString()));

    final Schema invalidInput = Schema.recordOf("input",
                                                Schema.Field.of("a", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("c", Schema.of(Schema.Type.STRING)));

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(invalidInput);
    decoder.configurePipeline(mockPipelineConfigurer);
  }
}
