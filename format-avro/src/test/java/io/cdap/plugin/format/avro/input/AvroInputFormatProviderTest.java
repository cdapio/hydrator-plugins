/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.format.avro.input;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.SchemaDetector;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link AvroInputFormatProvider}
 */
public class AvroInputFormatProviderTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testSchemaDetection() throws IOException {
    // write avro file
    Schema expectedSchema = Schema.recordOf("Avro",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(expectedSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .build();

    File avroFile = new File(TMP_FOLDER.newFolder(), "test.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, avroFile);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // call detect schema
    AvroInputFormatProvider.Conf conf = new AvroInputFormatProvider.Conf();
    AvroInputFormatProvider formatProvider = new AvroInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    Schema schema = schemaDetector.detectSchema(avroFile.getPath(), formatContext, Collections.emptyMap());
    Assert.assertEquals(expectedSchema, schema);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testPathFieldAddition() throws IOException {
    // write avro file
    Schema expectedSchema = Schema.recordOf("Avro",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
                                            Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    Schema inputSchema = Schema.recordOf("Avro",
                                          Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(inputSchema.toString());
    File avroFile = new File(TMP_FOLDER.newFolder(), "testPathFieldAddition.avro");
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .build();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, avroFile);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // call detect schema
    AvroInputFormatProvider.Conf conf = new AvroInputFormatProvider.Conf("pathField");
    AvroInputFormatProvider formatProvider = new AvroInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    Schema schema = schemaDetector.detectSchema(avroFile.getPath(), formatContext, Collections.emptyMap());
    Assert.assertEquals(expectedSchema, schema);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testPathFieldConflict() throws IOException {
    // write avro file
    Schema expectedSchema = Schema.recordOf("Avro",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("test", Schema.of(Schema.Type.STRING)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(expectedSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .set("test", "value")
      .build();

    File avroFile = new File(TMP_FOLDER.newFolder(), "test.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, avroFile);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // call detect schema
    AvroInputFormatProvider.Conf conf = new AvroInputFormatProvider.Conf("test");
    AvroInputFormatProvider formatProvider = new AvroInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    schemaDetector.detectSchema(avroFile.getPath(), formatContext, Collections.emptyMap());
    Assert.assertEquals(formatContext.getFailureCollector().getValidationFailures().size(), 1);
    ValidationFailure validationFailure = formatContext.getFailureCollector().getValidationFailures().get(0);
    List<ValidationFailure.Cause> actualCauses = validationFailure.getCauses();
    ValidationFailure.Cause expectedCause = new ValidationFailure.Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, "pathField");
    List<ValidationFailure.Cause> expectedCauses = new ArrayList<>();
    expectedCauses.add(expectedCause);
    Assert.assertEquals(expectedCauses, actualCauses);
  }
}
