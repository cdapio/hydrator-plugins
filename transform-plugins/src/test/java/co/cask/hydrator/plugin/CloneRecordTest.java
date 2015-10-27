package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link CloneRecord}.
 */
public class CloneRecordTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT = Schema.recordOf("output",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  @Test
  public void testCloneRecord() throws Exception {
    CloneRecord.Config config = new CloneRecord.Config(5);
    Transform<StructuredRecord, StructuredRecord> transform = new CloneRecord(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("a", "1")
                          .set("b", "2")
                          .set("c", "3")
                          .set("d", "4")
                          .set("e", "5").build(), emitter);
    Assert.assertEquals(5, emitter.getEmitted().size());
  }
}