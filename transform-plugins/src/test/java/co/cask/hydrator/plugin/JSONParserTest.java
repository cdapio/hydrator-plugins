package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link JSONParser}
 */
public class JSONParserTest {
  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT2 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));
  @Test
  public void testJSONParser() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "{\"a\": \"1\", \"b\": \"2\", \"c\" : \"3\", \"d\" : \"4\", \"e\" : \"5\" }")
                          .build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
  }

  @Test
  public void testJSONParserProjections() throws Exception {
    JSONParser.Config config = new JSONParser.Config("body", OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new JSONParser(config);
    transform.initialize(null);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "{\"a\": \"1\", \"b\": \"2\", \"c\" : \"3\", \"d\" : \"4\", \"e\" : \"5\" }")
                          .build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
  }
}