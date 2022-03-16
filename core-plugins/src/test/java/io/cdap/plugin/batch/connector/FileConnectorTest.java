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
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseEntityTypeInfo;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.SamplePropertyField;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.batch.source.FileBatchSource;
import io.cdap.plugin.batch.source.FileSourceConfig;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  static final List<String> SAMPLE_FIELD_NAMES = Arrays.asList(SAMPLE_FORMAT_KEY, SAMPLE_DELIMITER_KEY,
                                                               SAMPLE_FILE_ENCODING_KEY, SAMPLE_SKIP_HEADER_KEY,
                                                               SAMPLE_ENABLE_QUOTED_VALUES_KEY);


  private static final Set<BrowseEntityTypeInfo> SAMPLE_PROPERTIES = new HashSet<>();

  {
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
    SAMPLE_PROPERTIES.add(new BrowseEntityTypeInfo("file", browseEntityTypeInfoList));
  }

  private static FileConnector fileConnector = new FileConnector(new FileConnector.FileConnectorConfig());
  private static ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());

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
}
