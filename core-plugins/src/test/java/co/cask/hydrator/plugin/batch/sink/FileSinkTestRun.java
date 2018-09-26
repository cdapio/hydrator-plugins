/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.metadata.MetadataAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.google.common.collect.ImmutableMap;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

  @BeforeClass
  public static void setUp() {
    metadataAdmin = getMetadataAdmin();
  }

  @Test
  public void testTextFileSink() throws Exception {
    File outputDir = runPipeline("text");

    Map<Integer, String> output = new HashMap<>();
    for (File outputFile : outputDir.listFiles()) {
      String fileName = outputFile.getName();
      if (fileName.startsWith(".") || "_SUCCESS".equals(fileName)) {
        continue;
      }
      try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split(",");
          output.put(Integer.valueOf(fields[0]), fields[1]);
        }
      }
    }
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema("text");
  }

  private void validateDatasetSchema(String format) {
    // if a schema was provided for the sink verify that the external dataset has the given schema
    Map<String, String> metadataProperties =
      metadataAdmin.getProperties(MetadataScope.SYSTEM,
                                  MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(),
                                                           getReferenceName(format)));
    Assert.assertEquals(SCHEMA.toString(), metadataProperties.get(DatasetProperties.SCHEMA));
  }

  @Test
  public void testAvroFileSink() throws Exception {
    File outputDir = runPipeline("avro");
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(SCHEMA.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);

    Map<Integer, String> output = new HashMap<>();
    for (File outputFile : outputDir.listFiles()) {
      String fileName = outputFile.getName();
      if (fileName.startsWith(".") || "_SUCCESS".equals(fileName)) {
        continue;
      }

      try (DataFileStream<GenericRecord> fileStream =
        new DataFileStream<>(new FileInputStream(outputFile), datumReader)) {

        for (GenericRecord genericRecord : fileStream) {
          output.put((int) genericRecord.get("i"), genericRecord.get("s").toString());
        }
      }
    }
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema("avro");
  }

  @Test
  public void testParquetFileSink() throws Exception {
    File outputDir = runPipeline("parquet");

    Map<Integer, String> output = new HashMap<>();
    for (File outputFile : outputDir.listFiles()) {
      String fileName = outputFile.getName();
      if (fileName.startsWith(".") || "_SUCCESS".equals(fileName)) {
        continue;
      }

      Path parquetFile = new Path(outputFile.toString());
      AvroParquetReader.Builder<GenericRecord> genericRecordBuilder = AvroParquetReader.builder(parquetFile);
      try (ParquetReader<GenericRecord> reader = genericRecordBuilder.build()) {
        GenericRecord genericRecord = reader.read();
        while (genericRecord != null) {
          output.put((int) genericRecord.get("i"), genericRecord.get("s").toString());
          genericRecord = reader.read();
        }
      }
    }
    Assert.assertEquals(ImmutableMap.of(0, "abc", 1, "def", 2, "ghi"), output);
    validateDatasetSchema("parquet");
  }

  /**
   * Creates and runs a pipeline that is the mock source writing to a file sink using the specified format.
   * It will always write three records, {"i":0, "s":"abc"}, {"i":1, "s":"def"}, and {"i":2, "s":"ghi"}.
   * The output directory will be returned.
   */
  private File runPipeline(String format) throws Exception {
    String inputName = UUID.randomUUID().toString();

    File baseDir = TEMP_FOLDER.newFolder(format + "FileSink");
    File outputDir = new File(baseDir, "out");
    Map<String, String> properties = ImmutableMap.of("path", outputDir.getAbsolutePath(),
                                                     "referenceName", getReferenceName(format),
                                                     "format", format,
                                                     "schema", "${schema}");

    ETLBatchConfig conf = ETLBatchConfig.builder("* * * * *")
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
    return outputDir;
  }

  private String getReferenceName(String format) {
    return format + "Files";
  }
}

