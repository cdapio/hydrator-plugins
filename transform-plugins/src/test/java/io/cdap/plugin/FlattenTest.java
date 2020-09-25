 /*
  * Copyright © 2016-2020 Cask Data, Inc.
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
 package io.cdap.plugin;

 import io.cdap.cdap.api.data.format.StructuredRecord;
 import io.cdap.cdap.api.data.schema.Schema;
 import io.cdap.cdap.etl.api.Transform;
 import io.cdap.cdap.etl.api.validation.ValidationException;
 import io.cdap.cdap.etl.mock.common.MockEmitter;
 import io.cdap.cdap.etl.mock.transform.MockTransformContext;
 import org.junit.Assert;
 import org.junit.Test;

 /**
  * Tests {@link Flatten}.
  */
 public class FlattenTest {

   private static final Schema INPUT_SCHEMA_RECORD_A =
     Schema.recordOf("recordA",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", Schema.of(Schema.Type.STRING))
     );

   private static final Schema INPUT_SCHEMA_RECORD_B =
     Schema.recordOf("recordB",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", INPUT_SCHEMA_RECORD_A));

   private static final Schema INPUT_SCHEMA_RECORD_B_DUPLICATE =
     Schema.recordOf("recordBDuplicate",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", INPUT_SCHEMA_RECORD_A),
                     Schema.Field.of("b_a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b_b", Schema.of(Schema.Type.STRING)));


   private static final Schema INPUT = Schema.recordOf("input",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("c", INPUT_SCHEMA_RECORD_A),
                                                       Schema.Field.of("d", INPUT_SCHEMA_RECORD_B));

   @Test
   public void testWhenNoFieldIsSelected() {
     Flatten.Config config = new Flatten.Config(null, "10", "prefix");
     Transform<StructuredRecord, StructuredRecord> transform = new Flatten(config);
     MockTransformContext context = new MockTransformContext();
     try {
       transform.initialize(context);
       Assert.assertEquals(1, context.getFailureCollector().getValidationFailures().size());
     } catch (Exception e) {
       Assert.assertEquals(e.getClass(), ValidationException.class);
       Assert.assertEquals(1, ((ValidationException) e).getFailures().size());
     }
   }

   @Test
   public void testSchemaGeneration() throws Exception {
     String fieldsToFlatten = "a,b,c,d";
     Flatten.Config config = new Flatten.Config(fieldsToFlatten, "10", "prefix");
     Transform<StructuredRecord, StructuredRecord> transform = new Flatten(config);
     MockTransformContext context = new MockTransformContext();
     transform.initialize(context);
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
     transform.transform(StructuredRecord.builder(INPUT)
                           .set("a", "1")
                           .set("b", "2")
                           .set("c",
                                StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                  .set("a", "a1")
                                  .set("b", "b1").build()
                           )
                           .set("d", StructuredRecord.builder(INPUT_SCHEMA_RECORD_B)
                             .set("a", "a2")
                             .set("b",
                                  StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                    .set("a", "a1")
                                    .set("b", "b1").build()
                             ).build()
                           ).build(), emitter);
     Assert.assertEquals(1, emitter.getEmitted().size());
     Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
     Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
     Assert.assertEquals("a1", emitter.getEmitted().get(0).get("c_a"));
     Assert.assertEquals("a1", emitter.getEmitted().get(0).get("d_b_a"));
     Assert.assertEquals("b1", emitter.getEmitted().get(0).get("c_b"));
     Assert.assertEquals("a2", emitter.getEmitted().get(0).get("d_a"));
     Assert.assertEquals("b1", emitter.getEmitted().get(0).get("d_b_b"));
   }

   @Test
   public void testDuplicates() throws Exception {
     String fieldsToFlatten = "a,b,c,d";
     Flatten.Config config = new Flatten.Config(fieldsToFlatten, "10", "prefix");
     Transform<StructuredRecord, StructuredRecord> transform = new Flatten(config);
     MockTransformContext context = new MockTransformContext();
     transform.initialize(context);
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

     transform.transform(StructuredRecord.builder(INPUT_SCHEMA_RECORD_B_DUPLICATE)
                           .set("a", "a2")
                           .set("b", StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                             .set("a", "a1")
                             .set("b", "b1").build()
                           ).set("b_a", "duplicate1")
                           .set("b_b", "duplicate2")
                           .build(), emitter);

     Assert.assertEquals(1, emitter.getEmitted().size());
     Assert.assertEquals("duplicate1", emitter.getEmitted().get(0).get("prefix_b_a"));
     Assert.assertEquals("duplicate2", emitter.getEmitted().get(0).get("prefix_b_b"));

   }

   @Test
   public void testLevelLimit() throws Exception {
     String fieldsToFlatten = "a,b,c,d";
     Flatten.Config config = new Flatten.Config(fieldsToFlatten, null, "prefix");
     Transform<StructuredRecord, StructuredRecord> transform = new Flatten(config);
     MockTransformContext context = new MockTransformContext();
     transform.initialize(context);
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
     transform.transform(StructuredRecord.builder(INPUT)
                           .set("a", "1")
                           .set("b", "2")
                           .set("c",
                                StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                  .set("a", "a1")
                                  .set("b", "b1").build()
                           )
                           .set("d", StructuredRecord.builder(INPUT_SCHEMA_RECORD_B)
                             .set("a", "a2")
                             .set("b",
                                  StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                    .set("a", "a1")
                                    .set("b", "b1").build()
                             ).build()
                           ).build(), emitter);
     Assert.assertEquals(1, emitter.getEmitted().size());
     Assert.assertEquals(6, emitter.getEmitted().get(0).getSchema().getFields().size());
   }
 }
