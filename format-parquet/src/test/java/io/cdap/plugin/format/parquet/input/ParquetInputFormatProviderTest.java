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

package io.cdap.plugin.format.parquet.input;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.SchemaDetector;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
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
 * Tests for {@link ParquetInputFormatProvider}.
 */
public class ParquetInputFormatProviderTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testSchemaDetection() throws IOException {
    Configuration hConf = new Configuration();
    File parquetFile = new File(TMP_FOLDER.newFolder(), "test.parquet");
    Path parquetPath = new Path(parquetFile.toURI());
    Schema expectedSchema = Schema.recordOf("x",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(expectedSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .build();

    ParquetWriter<GenericRecord> parquetWriter =
      AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(parquetPath, hConf))
        .withSchema(avroSchema)
        .build();
    parquetWriter.write(record);
    parquetWriter.close();

    ParquetInputFormatProvider.Conf conf = new ParquetInputFormatProvider.Conf();
    ParquetInputFormatProvider formatProvider = new ParquetInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    Schema schema = schemaDetector.detectSchema(parquetFile.getAbsolutePath(), formatContext, Collections.emptyMap());
    Assert.assertEquals(expectedSchema, schema);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testPathFieldAddition() throws IOException {
    Configuration hConf = new Configuration();
    File parquetFile = new File(TMP_FOLDER.newFolder(), "pathFieldTest.parquet");
    Path parquetPath = new Path(parquetFile.toURI());
    Schema expectedSchema = Schema.recordOf("x",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
                                            Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    Schema inputSchema = Schema.recordOf("x",
                                         Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(inputSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .build();

    ParquetWriter<GenericRecord> parquetWriter =
      AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(parquetPath, hConf))
        .withSchema(avroSchema)
        .build();
    parquetWriter.write(record);
    parquetWriter.close();

    ParquetInputFormatProvider.Conf conf = new ParquetInputFormatProvider.Conf("pathField");
    ParquetInputFormatProvider formatProvider = new ParquetInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    Schema schema = schemaDetector.detectSchema(parquetFile.getAbsolutePath(), formatContext, Collections.emptyMap());
    Assert.assertEquals(expectedSchema, schema);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testPathFieldConflict() throws IOException {
    Configuration hConf = new Configuration();
    File parquetFile = new File(TMP_FOLDER.newFolder(), "test.parquet");
    Path parquetPath = new Path(parquetFile.toURI());
    Schema expectedSchema = Schema.recordOf("x",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
                                            Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(expectedSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 0)
      .set("name", "alice")
      .set("score", 1.0d)
      .set("pathField", "value")
      .build();
    ParquetWriter<GenericRecord> parquetWriter =
      AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(parquetPath, hConf))
        .withSchema(avroSchema)
        .build();
    parquetWriter.write(record);
    parquetWriter.close();

    ParquetInputFormatProvider.Conf conf = new ParquetInputFormatProvider.Conf("pathField");
    ParquetInputFormatProvider formatProvider = new ParquetInputFormatProvider(conf);
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(formatProvider);
    schemaDetector.detectSchema(parquetFile.getAbsolutePath(), formatContext, Collections.emptyMap());
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
