package co.cask.hydrator.plugin;


import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Tests {@link Decompressor}
 */
public class DecompressorTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.BYTES)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)));

  @Test
  public void testSnappyCompress() throws Exception {
    String decompressTester = "This is a test for testing snappy compression";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Decompressor(new Decompressor.Config("a:SNAPPY", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    byte[] compressed = Snappy.compress(decompressTester.getBytes());
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", compressed)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    String actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(decompressTester, actual);
  }

  @Test
  public void testZipCompress() throws Exception {
    String decompressTester = "This is a test for testing zip compression";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Decompressor(new Decompressor.Config("a:ZIP", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    byte[] compressed = zip(decompressTester.getBytes());
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", compressed)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    String actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(decompressTester, actual);
  }

  @Test
  public void testGZipCompress() throws Exception {
    String decompressTester = "This is a test for testing gzip compression";
    Transform<StructuredRecord, StructuredRecord> transform =
      new Decompressor(new Decompressor.Config("a:GZIP", OUTPUT.toString()));
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    byte[] compressed = gzip(decompressTester.getBytes());
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", compressed)
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    String actual = emitter.getEmitted().get(0).get("a");
    Assert.assertEquals(2, emitter.getEmitted().get(0).getSchema().getFields().size());
    Assert.assertEquals(decompressTester, actual);
  }


  private static byte[] gzip(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(input, 0, input.length);
    gzip.flush();
    gzip.close();
    return out.toByteArray();
  }

  private byte[] zip(byte[] input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(out);
    zos.setLevel(9);  
    zos.putNextEntry(new ZipEntry("c"));
    zos.write(input, 0, input.length);
    zos.flush();
    zos.close();
    return out.toByteArray();
  }
}