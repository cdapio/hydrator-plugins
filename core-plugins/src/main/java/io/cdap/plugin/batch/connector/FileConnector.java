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
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.dataset.lib.FileSet;
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
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * File connector to browse flat file on local system
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(FileConnector.NAME)
@Description("Connection to browse and sample data from the local file system.")
@Category("File")
@Requirements(datasetTypes = FileSet.TYPE)
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
    Path path = new Path(request.getPath());
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    // need this to load the extra class loader to avoid ClassNotFoundException for the file system
    FileSystem fs = JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                                       f -> FileSystem.get(path.toUri(), conf));

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(String.format("The given path %s does not exist", path));
    }

    // if it is not a directory, then it is not browsable, return the path itself
    FileStatus file = fs.getFileStatus(path);
    if (!file.isDirectory()) {
      return BrowseDetail.builder().setTotalCount(1).addEntity(generateBrowseEntity(file)).build();
    }

    FileStatus[] files = fs.listStatus(path);
    Arrays.sort(files);

    int count = 0;
    int limit = request.getLimit() == null ? Integer.MAX_VALUE : request.getLimit();
    BrowseDetail.Builder builder = BrowseDetail.builder();
    for (FileStatus fileStatus : files) {
      // do not browse hidden files
      if (fileStatus.getPath().getName().startsWith(".")) {
        continue;
      }

      if (count >= limit) {
        break;
      }

      builder.addEntity(generateBrowseEntity(fileStatus));
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

  private BrowseEntity generateBrowseEntity(FileStatus file) throws IOException {
    Path path = file.getPath();
    boolean isDirectory = file.isDirectory();
    String filePath = path.toUri().getPath();
    BrowseEntity.Builder builder = BrowseEntity.builder(
      path.getName(), filePath, isDirectory ? DIRECTORY_TYPE : FILE_TYPE);

    if (isDirectory) {
      builder.canBrowse(true).canSample(true);
    } else {
      String fileType = FileTypeDetector.detectFileType(filePath);
      builder.canSample(FileTypeDetector.isSampleable(fileType));
      builder.addProperty(FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
        fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
      builder.addProperty(SIZE_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(file.getLen()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
    }

    builder.addProperty(LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
      String.valueOf(file.getModificationTime()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
    String owner = file.getOwner();
    builder.addProperty(OWNER_KEY, BrowseEntityPropertyValue.builder(
      owner == null ? "" : owner, BrowseEntityPropertyValue.PropertyType.STRING).build());
    builder.addProperty(GROUP_KEY, BrowseEntityPropertyValue.builder(
      file.getGroup(), BrowseEntityPropertyValue.PropertyType.STRING).build());
    FsPermission permission = file.getPermission();
    String perm =
      permission.getUserAction().SYMBOL + permission.getGroupAction().SYMBOL + permission.getOtherAction().SYMBOL;
    builder.addProperty(PERMISSION_KEY, BrowseEntityPropertyValue.builder(
      perm, BrowseEntityPropertyValue.PropertyType.STRING).build());
    return builder.build();
  }

  /**
   * {@link PluginConfig} for {@link FileConnector}, currently this class is empty but can be extended later if
   * we want to support more configs, for example, a scheme to support any file system.
   */
  public static class FileConnectorConfig extends PluginConfig {
  }
}
