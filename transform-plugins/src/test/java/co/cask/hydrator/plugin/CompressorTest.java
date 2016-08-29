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
import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Tests {@link Compressor}.
 */
public class CompressorTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.BYTES)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
  
  @Test
  public void testSnappyCompress() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform = 
      new Compressor(new Compressor.Config("a:SNAPPY", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "This is a test for testing snappy compression")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    byte[] expected = Snappy.compress("This is a test for testing snappy compression".getBytes());
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testZipCompress() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Compressor(new Compressor.Config("a:ZIP", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "This is a test for testing zip compression")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    byte[] expected = compressZIP("This is a test for testing zip compression".getBytes());
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testGZIPCompress() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Compressor(new Compressor.Config("a:GZIP", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "This is a test for testing gzip compression")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);

    byte[] expected = compressGZIP("This is a test for testing gzip compression".getBytes());
    byte[] actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testSchemaValidation() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Compressor(new Compressor.Config("a:GZIP", OUTPUT.toString()));
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(INPUT);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT, mockPipelineConfigurer.getOutputSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSchemaValidationWithInvalidInputSchema() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Compressor(new Compressor.Config("a:ZIP", OUTPUT.toString()));
    Schema invalidInput = Schema.recordOf("input",
                                          Schema.Field.of("a", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(invalidInput);
    transform.configurePipeline(mockPipelineConfigurer);
  }

  @Test
  public void testSchemaValidationWithValidInputSchema() throws Exception {
    Transform<StructuredRecord, StructuredRecord> transform =
      new Compressor(new Compressor.Config("a:NONE", OUTPUT.toString()));
    Schema validInput = Schema.recordOf("input",
                                        Schema.Field.of("a", Schema.of(Schema.Type.INT)),
                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(validInput);
    transform.configurePipeline(mockPipelineConfigurer);
    Assert.assertEquals(OUTPUT, mockPipelineConfigurer.getOutputSchema());
  }

  private static byte[] compressGZIP(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(input, 0, input.length);
    gzip.close();
    return out.toByteArray();
  }

  private byte[] compressZIP(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(out);
    zos.putNextEntry(new ZipEntry("c"));
    zos.write(input, 0, input.length);
    zos.close();
    return out.toByteArray();
  }
}
