/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.batch.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseEntityTypeInfo;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SamplePropertyField;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.plugin.batch.source.FileBatchSource;
import io.cdap.plugin.batch.source.FileSourceConfig;
import io.cdap.plugin.format.avro.input.AvroInputFormatProvider;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.format.delimited.input.CSVInputFormatProvider;
import io.cdap.plugin.format.delimited.input.DelimitedConfig;
import io.cdap.plugin.format.delimited.input.DelimitedInputFormatProvider;
import io.cdap.plugin.format.delimited.input.TSVInputFormatProvider;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.parquet.input.ParquetInputFormatProvider;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Test for {@link FileConnector}
 */
public class FileConnectorTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String SAMPLE_FORMAT_KEY = "format";
  private static final String SAMPLE_DELIMITER_KEY = "delimiter";
  private static final String SAMPLE_FILE_ENCODING_KEY = "fileEncoding";
  private static final String SAMPLE_FILE_ENCODING_DEFAULT = "UTF-8";
  private static final String SAMPLE_SKIP_HEADER_KEY = "skipHeader";
  private static final String SAMPLE_ENABLE_QUOTED_VALUES_KEY = "enableQuotedValues";
  private static final String PLUGIN_NAME_PROPERTY_KEY = "_pluginName";
  private static final String SAMPLE_NAME_SCHEMA = "schema";

  static final List<String> SAMPLE_FIELD_NAMES = Arrays.asList(SAMPLE_FORMAT_KEY, SAMPLE_DELIMITER_KEY,
                                                               SAMPLE_FILE_ENCODING_KEY, SAMPLE_SKIP_HEADER_KEY,
                                                               SAMPLE_ENABLE_QUOTED_VALUES_KEY, SAMPLE_NAME_SCHEMA);


  private static final Set<BrowseEntityTypeInfo> SAMPLE_PROPERTIES = new HashSet<>();

  @BeforeClass
  public static void setupTestClass() {
    List<Field> sourceFields = new ArrayList<>();
    for (String sourceFieldName : SAMPLE_FIELD_NAMES) {
      Class<?> fileClass = FileSourceConfig.class;
      while (fileClass != null) {
        try {
          sourceFields.add(fileClass.getDeclaredField(sourceFieldName));
          break;
        } catch (NoSuchFieldException e) {
          // Check if it's defined in its super class.
          fileClass = fileClass.getSuperclass();
        }
        // Skip sample option as it's not implemented by the source
      }
    }

    List<SamplePropertyField> browseEntityTypeInfoList = new ArrayList<>();
    for (Field field: sourceFields) {
      browseEntityTypeInfoList.add(
        new SamplePropertyField(field.getName(),
                                field.getDeclaredAnnotation(Description.class).value()));
    }

    browseEntityTypeInfoList.add(
      new SamplePropertyField(PLUGIN_NAME_PROPERTY_KEY, FileBatchSource.NAME)
    );

    SAMPLE_PROPERTIES.add(new BrowseEntityTypeInfo("file", browseEntityTypeInfoList));
  }

  @Test
  public void testFileConnectorBrowse() throws Exception {
    List<BrowseEntity> entities = new ArrayList<>();
    // add files
    File directory = TEMP_FOLDER.newFolder();
    for (int i = 0; i < 5; i++) {
      // create 5 text files
      File file = new File(directory, "file" + i + ".txt");
      file.createNewFile();
      entities.add(BrowseEntity.builder(file.getName(), file.getCanonicalPath(), "file").canSample(true)
                               .setProperties(generateFileProperties(file, "text/plain")).build());
    }

    // test different extensions
    Map<String, String> fileExtensionsMimetype = new HashMap<String, String>() {{
        put("txt", "text/plain");
        put("csv", "text/csv");
        put("tsv", "text/tab-separated-values");
        put("avro", "application/avro");
        put("parquet", "application/parquet");
        put("json", "application/json");
      }};

    // Adding integer to maintain order between browsing and generated files.
    int count = 0;
    for (Map.Entry<String, String> entry: fileExtensionsMimetype.entrySet()) {
      File file = new File(directory, "filemime" + count + "." + entry.getKey());
      count++;
      file.createNewFile();
      entities.add(BrowseEntity.builder(file.getName(), file.getCanonicalPath(), "file").canSample(true)
                     .setProperties(generateFileProperties(file, entry.getValue())).build());
    }

    // add directory
    for (int i = 0; i < 5; i++) {
      File folder = new File(directory, "folder" + i);
      folder.mkdir();
      entities.add(
        BrowseEntity.builder(folder.getName(), folder.getCanonicalPath(), "directory").canBrowse(true).canSample(true)
          .setProperties(generateFileProperties(folder, null)).build());
    }

    FileConnector fileConnector = new FileConnector(new FileConnector.FileConnectorConfig());
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer(Collections.emptyMap()));
    BrowseDetail detail = fileConnector.browse(context, BrowseRequest.builder(directory.getCanonicalPath()).build());
    Assert.assertEquals(BrowseDetail.builder().setTotalCount(entities.size()).setEntities(entities)
                          .setSampleProperties(SAMPLE_PROPERTIES).build(), detail);

    // test limit
    detail = fileConnector.browse(context, BrowseRequest.builder(directory.getCanonicalPath()).setLimit(5).build());
    Assert.assertEquals(BrowseDetail.builder().setTotalCount(5).setEntities(entities.subList(0, 5))
                          .setSampleProperties(SAMPLE_PROPERTIES).build(), detail);

    // test browse a file
    BrowseDetail single = fileConnector.browse(
      context, BrowseRequest.builder(new File(directory, "file0.txt").getCanonicalPath()).build());
    Assert.assertEquals(BrowseDetail.builder().setTotalCount(1).setEntities(entities.subList(0, 1))
                          .setSampleProperties(SAMPLE_PROPERTIES).build(), single);

    // test browse empty directory
    BrowseDetail empty = fileConnector.browse(
      context, BrowseRequest.builder(new File(directory, "folder0").getCanonicalPath()).build());
    Assert.assertEquals(BrowseDetail.builder().setSampleProperties(SAMPLE_PROPERTIES).build(), empty);
  }

  private Map<String, BrowseEntityPropertyValue> generateFileProperties(File file,
                                                                        @Nullable String fileType) throws Exception {
    Map<String, BrowseEntityPropertyValue> properties = new HashMap<>();
    if (file.isFile()) {
      properties.put(FileConnector.FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
        fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
      properties.put(FileConnector.SIZE_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(file.length()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
    }
    properties.put(FileConnector.LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
      String.valueOf(file.lastModified()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
    properties.put(FileConnector.OWNER_KEY, BrowseEntityPropertyValue.builder(
      Files.getOwner(file.toPath()).getName(), BrowseEntityPropertyValue.PropertyType.STRING).build());
    properties.put(FileConnector.GROUP_KEY, BrowseEntityPropertyValue.builder(
      Files.readAttributes(file.toPath(), PosixFileAttributes.class).group().getName(),
      BrowseEntityPropertyValue.PropertyType.STRING).build());
    properties.put(FileConnector.PERMISSION_KEY, BrowseEntityPropertyValue.builder(
       PosixFilePermissions.toString(Files.getPosixFilePermissions(file.toPath())),
      BrowseEntityPropertyValue.PropertyType.STRING).build());

    if (file.isFile()) {
      properties.put(SAMPLE_FORMAT_KEY, BrowseEntityPropertyValue.builder(
        FileTypeDetector.detectFileFormat(FileTypeDetector.detectFileType(file.getName())).name().toLowerCase(),
        BrowseEntityPropertyValue.PropertyType.SAMPLE_DEFAULT).build());
      properties.put(SAMPLE_FILE_ENCODING_KEY, BrowseEntityPropertyValue.builder(
        SAMPLE_FILE_ENCODING_DEFAULT, BrowseEntityPropertyValue.PropertyType.SAMPLE_DEFAULT).build());
    }

    return properties;
  }

  @Test
  public void testSchemaDetectionParquet() throws IOException {
    Configuration hConf = new Configuration();
    File parquetFile = new File(TEMP_FOLDER.newFolder(), "test.parquet");
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
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
    parquetWriter.write(record);
    parquetWriter.close();

    ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder()
      .setPath(parquetFile.getAbsolutePath())
      .build();

    FileConnector fileConnector = new FileConnector(new FileConnector.FileConnectorConfig());
    ParquetInputFormatProvider plugin = new ParquetInputFormatProvider(new ParquetInputFormatProvider.Conf());
    ConnectorContext context = new TestConnectorContext(plugin);
    ConnectorSpec spec = fileConnector.generateSpec(context, connectorSpecRequest);
    Assert.assertEquals(expectedSchema, spec.getSchema());

    SampleRequest sampleRequest = SampleRequest.builder(10)
      .setPath(parquetFile.getAbsolutePath())
      .build();
    InputFormatProvider inputFormatProvider = fileConnector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> formatConfigs = inputFormatProvider.getInputFormatConfiguration();
    Assert.assertTrue(formatConfigs.containsKey(PathTrackingInputFormat.SCHEMA));
    Assert.assertEquals(expectedSchema, Schema.parseJson(formatConfigs.get(PathTrackingInputFormat.SCHEMA)));
  }

  @Test
  public void testSchemaDetectionAvro() throws IOException {
    File avroFile = new File(TEMP_FOLDER.newFolder(), "test.avro");

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

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, avroFile);
    dataFileWriter.append(record);
    dataFileWriter.close();

    ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder()
      .setPath(avroFile.getAbsolutePath())
      .build();

    FileConnector fileConnector = new FileConnector(new FileConnector.FileConnectorConfig());
    AvroInputFormatProvider plugin = new AvroInputFormatProvider(new AvroInputFormatProvider.Conf());
    ConnectorContext context = new TestConnectorContext(plugin);
    ConnectorSpec spec = fileConnector.generateSpec(context, connectorSpecRequest);
    Assert.assertEquals(expectedSchema, spec.getSchema());

    SampleRequest sampleRequest = SampleRequest.builder(10)
      .setPath(avroFile.getAbsolutePath())
      .build();
    InputFormatProvider inputFormatProvider = fileConnector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> formatConfigs = inputFormatProvider.getInputFormatConfiguration();
    Assert.assertTrue(formatConfigs.containsKey(PathTrackingInputFormat.SCHEMA));
    Assert.assertEquals(expectedSchema, Schema.parseJson(formatConfigs.get(PathTrackingInputFormat.SCHEMA)));
  }

  @Test
  public void testSchemaDetectionDelimited() throws IOException {
    testSchemaDetectionDelimited("|", new DelimitedInputFormatProvider(new DelimitedInputFormatProvider.Conf("|")));
  }

  @Test
  public void testSchemaDetectionCSV() throws IOException {
    testSchemaDetectionDelimited(",", new CSVInputFormatProvider(new DelimitedConfig()));
  }

  @Test
  public void testSchemaDetectionTSV() throws IOException {
    testSchemaDetectionDelimited("\t", new TSVInputFormatProvider(new DelimitedConfig()));
  }

  private void testSchemaDetectionDelimited(String delimiter, InputFormatProvider plugin) throws IOException {
    File file = TEMP_FOLDER.newFile(UUID.randomUUID() + ".txt");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(String.format("a%sb%sc", delimiter, delimiter).getBytes(StandardCharsets.UTF_8));
    }

    ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder()
      .setPath(file.getAbsolutePath())
      .build();

    FileConnector fileConnector = new FileConnector(new FileConnector.FileConnectorConfig());
    ConnectorContext context = new TestConnectorContext(plugin);
    ConnectorSpec spec = fileConnector.generateSpec(context, connectorSpecRequest);

    Schema expectedSchema = Schema.recordOf("text",
                                            Schema.Field.of("body_0", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("body_1", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("body_2", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expectedSchema, spec.getSchema());

    SampleRequest sampleRequest = SampleRequest.builder(10)
      .setPath(file.getAbsolutePath())
      .build();
    InputFormatProvider inputFormatProvider = fileConnector.getInputFormatProvider(context, sampleRequest);
    Map<String, String> formatConfigs = inputFormatProvider.getInputFormatConfiguration();
    Assert.assertTrue(formatConfigs.containsKey(PathTrackingInputFormat.SCHEMA));
    Assert.assertEquals(expectedSchema, Schema.parseJson(formatConfigs.get(PathTrackingInputFormat.SCHEMA)));
  }

  private static class TestConnectorContext implements ConnectorContext {
    private final FailureCollector failureCollector = new SimpleFailureCollector();
    private final PluginConfigurer pluginConfigurer;

    TestConnectorContext(Object plugin) {
      this.pluginConfigurer = new TestConnectorConfigurer(plugin);
    }

    public FailureCollector getFailureCollector() {
      return this.failureCollector;
    }

    public PluginConfigurer getPluginConfigurer() {
      return this.pluginConfigurer;
    }
  }

  @SuppressWarnings("unchecked")
  private static class TestConnectorConfigurer implements ConnectorConfigurer {
    private final Object plugin;

    TestConnectorConfigurer(Object plugin) {
      this.plugin = plugin;
    }

    @Nullable
    @Override
    public <T> T usePlugin(String s, String s1, String s2, PluginProperties pluginProperties,
                           PluginSelector pluginSelector) {
      return (T) plugin;
    }

    @Nullable
    @Override
    public <T> Class<T> usePluginClass(String s, String s1, String s2, PluginProperties pluginProperties,
                                       PluginSelector pluginSelector) {
      return (Class<T>) plugin.getClass();
    }
  }
}
