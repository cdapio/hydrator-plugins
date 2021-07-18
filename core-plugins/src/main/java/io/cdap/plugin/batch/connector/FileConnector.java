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

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.plugin.batch.source.FileBatchSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * File connector to browse flat file on local system
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(FileConnector.NAME)
@Description("File connector for local file system")
@Category("File")
public class FileConnector extends AbstractFileConnector<FileConnector.FileConnectorConfig> {
  public static final String NAME = "File";
  static final String FILE_TYPE_KEY = "File Type";
  static final String SIZE_KEY = "Size";
  static final String LAST_MODIFIED_KEY = "Last Modified";
  static final String OWNER_KEY = "Owner";
  static final String GROUP_KEY = "Group";
  static final String PERMISSION_KEY = "Permission";
  private static final String DIRECTORY_TYPE = "directory";
  private static final String FILE_TYPE = "file";

  public FileConnector(FileConnectorConfig config) {
    super(config);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
    String path = request.getPath();
    File file = new File(request.getPath());
    if (!file.exists()) {
      throw new IllegalArgumentException(String.format("The given path %s does not exist", path));
    }

    // if it is not a directory, then it is not browsable, return the path itself
    if (!file.isDirectory()) {
      return BrowseDetail.builder().setTotalCount(1).addEntity(generateBrowseEntity(file)).build();
    }

    // list the files and classify them with file and directory
    File[] files = file.listFiles();
    if (files == null) {
      throw new IOException(String.format("Unable to browse the path %s", path));
    }

    // sort the files by names
    Arrays.sort(files);
    int count = 0;
    int limit = request.getLimit() == null || request.getLimit() <= 0 ? files.length :
                  Math.min(request.getLimit(), files.length);
    BrowseDetail.Builder builder = BrowseDetail.builder();
    for (File value : files) {
      // do not browse hidden files
      if (value.isHidden()) {
        continue;
      }

      if (count >= limit) {
        break;
      }

      builder.addEntity(generateBrowseEntity(value));
      count++;
    }
    return builder.setTotalCount(count).build();
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, ConnectorSpec.Builder builder) {
    super.setConnectorSpec(request, builder);
    // not use ImmutableMap here in case any property is null
    Map<String, String> properties = new HashMap<>();
    properties.put("path", request.getPath());
    properties.put("format", FileTypeDetector.detectFileFormat(
      FileTypeDetector.detectFileType(request.getPath())).name().toLowerCase());
    properties.put(Constants.Reference.REFERENCE_NAME, new File(request.getPath()).getName());
    builder.addRelatedPlugin(new PluginSpec(FileBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties));
  }

  private BrowseEntity generateBrowseEntity(File file) throws IOException {
    String path = file.getCanonicalPath();
    boolean isDirectory = file.isDirectory();
    BrowseEntity.Builder builder = BrowseEntity.builder(file.getName(), path, isDirectory ? DIRECTORY_TYPE : FILE_TYPE);

    if (isDirectory) {
      builder.canBrowse(true).canSample(true);
    } else {
      String fileType = FileTypeDetector.detectFileType(path);
      builder.canSample(FileTypeDetector.isSampleable(fileType));
      builder.addProperty(FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
        fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
      builder.addProperty(SIZE_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(file.length()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
    }

    builder.addProperty(LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
      String.valueOf(file.lastModified()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
    UserPrincipal owner = Files.getOwner(file.toPath());
    builder.addProperty(OWNER_KEY, BrowseEntityPropertyValue.builder(
      owner == null ? "" : owner.getName(), BrowseEntityPropertyValue.PropertyType.STRING).build());
    builder.addProperty(GROUP_KEY, BrowseEntityPropertyValue.builder(
      Files.readAttributes(file.toPath(), PosixFileAttributes.class).group().getName(),
      BrowseEntityPropertyValue.PropertyType.STRING).build());
    builder.addProperty(PERMISSION_KEY, BrowseEntityPropertyValue.builder(
      PosixFilePermissions.toString(Files.getPosixFilePermissions(file.toPath())),
      BrowseEntityPropertyValue.PropertyType.STRING).build());
    return builder.build();
  }

  /**
   * {@link PluginConfig} for {@link FileConnector}, currently this class is empty but can be extended later if
   * we want to support more configs, for example, a scheme to support any file system.
   */
  public static class FileConnectorConfig extends PluginConfig {
  }
}
