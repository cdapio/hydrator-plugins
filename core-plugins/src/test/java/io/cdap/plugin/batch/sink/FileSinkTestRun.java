/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.format.FileFormat;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Test for {@link SnapshotFileBatchSink}.
 */
public class FileSinkTestRun extends ETLBatchTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Schema SCHEMA = Schema.recordOf("x",
                                                       Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                                       Schema.Field.of("s", Schema.of(Schema.Type.STRING)));
  private static MetadataAdmin metadataAdmin;

  /* Enum used to describe schema state and return schema values accordingly. */
  private enum SchemaState {
    EMPTY_STRING,
    NULL_VALUE,
    VALID,
    ;

    public String getSchema(String validSchema) {
      switch (this) {
        case EMPTY_STRING: return "";
        case NULL_VALUE: return null;
        case VALID: return validSchema;
        default: throw new IllegalArgumentException("Invalid schema state " + this);
      }
    }
  }

  @BeforeClass
  public static void setUp() {
    metadataAdmin = getMetadataAdmin();
  }

  @Test
  public void testCSVFileSink() throws Exception {
    testDelimitedFileSink(FileFormat.CSV, ",", false);
    testDelimitedFileSink(FileFormat.CSV, ",", true);
  }

  @Test
  public void testTSVFileSink() throws Exception {
    testDelimitedFileSink(FileFormat.TSV, "\t", false);
    testDelimitedFileSink(FileFormat.TSV, "\t", true);
  }

  @Test
  public void testDelimitedFileSink() throws Exception {
    testDelimitedFileSink(FileFormat.DELIMITED, "\u0001", false);
    testDelimitedFileSink(FileFormat.DELIMITED, "\u0001", true);
  }

  private void testDelimitedFileSink(FileFormat format, String delimiter, boolean writeHeader) throws Exception {
    // only set the delimiter as a pipeline property if the format is "delimited".
    // otherwise, the delimiter should be tied to the format
    Map<Integer, String> output = new HashMap<>();
    String expectedHeader = SCHEMA.getFields().stream()
      .map(Schema.Field::getName)
      .collect(Collectors.joining(delimiter));
    runPipeline(format, file -> {
                  try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    boolean isHeader = writeHeader;
                    while ((line = reader.readLine()) != null) {
                      if (isHeader) {
                        Assert.assertEquals(expectedHeader, line);
                        isHeader = false;
                        continue;
                      }
                      String[] fields = line.split(delimiter);
                      output.put(Integer.valueOf(fields[0]), fields[1]);
                    }
                  }
                }, format == FileFormat.DELIMITED ? delimiter : null,
                SchemaState.VALID,
                writeHeader);
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema(format);
  }

  @Test
  public void testJsonFileSink() throws Exception {
    Map<Integer, String> output = new HashMap<>();
    runPipeline(FileFormat.JSON, file -> {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = reader.readLine()) != null) {
          StructuredRecord outputRecord = StructuredRecordStringConverter.fromJsonString(line, SCHEMA);
          output.put(outputRecord.get("i"), outputRecord.get("s"));
        }
      }
    });
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema(FileFormat.AVRO);
  }

  @Test
  public void testAvroFileSink() throws Exception {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(SCHEMA.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);

    Map<Integer, String> output = new HashMap<>();
    runPipeline(FileFormat.AVRO, file -> {
      try (DataFileStream<GenericRecord> fileStream =
        new DataFileStream<>(new FileInputStream(file), datumReader)) {

        for (GenericRecord genericRecord : fileStream) {
          output.put((int) genericRecord.get("i"), genericRecord.get("s").toString());
        }
      }
    });
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema(FileFormat.AVRO);
  }

  @Test
  public void testParquetFileSink() throws Exception {
    testParquetFileSink(SchemaState.VALID);
  }

  @Test
  public void testParquetFileSink_outputFormatNull() throws Exception {
    try {
      testParquetFileSink(SchemaState.NULL_VALUE);
      Assert.fail("Pipeline should fail with IllegalArgumentException.");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testParquetFileSink_outputFormatEmptyString() throws Exception {
    try {
      testParquetFileSink(SchemaState.EMPTY_STRING);
      Assert.fail("Pipeline should fail.");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private void testParquetFileSink(SchemaState outputFormatSchemaState) throws Exception {
    Map<Integer, String> output = new HashMap<>();
    runPipeline(FileFormat.PARQUET, file -> {
      Path parquetFile = new Path(file.toString());
      AvroParquetReader.Builder<GenericRecord> genericRecordBuilder = AvroParquetReader.builder(parquetFile);
      try (ParquetReader<GenericRecord> reader = genericRecordBuilder.build()) {
        GenericRecord genericRecord = reader.read();
        while (genericRecord != null) {
          output.put((int) genericRecord.get("i"), genericRecord.get("s").toString());
          genericRecord = reader.read();
        }
      }
    }, null, outputFormatSchemaState, false);
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema(FileFormat.PARQUET);
  }

  private void runPipeline(FileFormat format, FileConsumer fileConsumer) throws Exception {
    runPipeline(format, fileConsumer, null);
  }

  private void runPipeline(FileFormat format, FileConsumer fileConsumer, @Nullable String delimiter)
      throws Exception {
    runPipeline(format, fileConsumer, delimiter, SchemaState.VALID, false);
  }

    /**
     * Creates and runs a pipeline that is the mock source writing to a file sink using the specified format.
     * It will always write three records, {"i":0, "s":"abc"}, {"i":1, "s":"def"}, and {"i":2, "s":"ghi"}.
     */
  private void runPipeline(
      FileFormat format,
      FileConsumer fileConsumer,
      @Nullable String delimiter,
      SchemaState outputFormatSchemaState,
      boolean writeHeader) throws Exception {
    String inputName = UUID.randomUUID().toString();

    File baseDir = TEMP_FOLDER.newFolder(
      String.format("%s-%s-FileSink-%s", format, writeHeader, outputFormatSchemaState));
    File outputDir = new File(baseDir, "out");
    Map<String, String> properties = new HashMap<>();
    properties.put("path", outputDir.getAbsolutePath());
    properties.put("referenceName", format.name());
    properties.put("format", format.name());
    properties.put("schema", outputFormatSchemaState.getSchema("${schema}"));
    properties.put("delimiter", delimiter);
    properties.put("writeHeader", String.valueOf(writeHeader));

    ETLBatchConfig conf = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputName, SCHEMA)))
      .addStage(new ETLStage("sink", new ETLPlugin("File", BatchSink.PLUGIN_TYPE, properties)))
      .addConnection("source", "sink")
      .build();

    ApplicationManager appManager = deployETL(conf, format + "FileSinkApp");

    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(SCHEMA).set("i", 0).set("s", "abc").build());
    input.add(StructuredRecord.builder(SCHEMA).set("i", 1).set("s", "def").build());
    input.add(StructuredRecord.builder(SCHEMA).set("i", 2).set("s", "ghi").build());
    MockSource.writeInput(inputManager, input);

    Map<String, String> arguments = new HashMap<>();
    arguments.put("schema", SCHEMA.toString());
    runETLOnce(appManager, arguments);

    File[] outputFiles = outputDir.listFiles();
    if (outputFiles == null) {
      return;
    }
    for (File outputFile : outputFiles) {
      String fileName = outputFile.getName();
      if (fileName.startsWith(".") || "_SUCCESS".equals(fileName)) {
        continue;
      }

      fileConsumer.consume(outputFile);
    }
  }

  private void validateDatasetSchema(FileFormat format) throws IOException {
    // if a schema was provided for the sink verify that the external dataset has the given schema
    Map<String, String> metadataProperties =
      metadataAdmin.getProperties(MetadataScope.SYSTEM,
                                  MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(),
                                                           format.name()));
    Assert.assertEquals(SCHEMA.toString(), metadataProperties.get(DatasetProperties.SCHEMA));
  }

  /**
   * Consumes a file.
   */
  private interface FileConsumer {
    void consume(File file) throws Exception;
  }
}

