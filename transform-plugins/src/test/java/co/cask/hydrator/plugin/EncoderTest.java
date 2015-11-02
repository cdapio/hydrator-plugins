package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.Snappy;

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

}