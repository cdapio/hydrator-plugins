/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * File source config
 */
public class FileSourceConfig extends AbstractFileSourceConfig {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";

  @Macro
  @Description("Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come"  +
    "from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where " +
    "value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'.")
  private String path;

  @Macro
  @Nullable
  @Description("Any additional properties to use when reading from the filesystem. "
    + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
  private String fileSystemProperties;

  FileSourceConfig() {
    super();
    fileSystemProperties = "{}";
  }

  @Override
  public void validate() {
    super.validate();
    getFileSystemProperties();
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    try {
      getFileSystemProperties();
    } catch (IllegalArgumentException e) {
      collector.addFailure("File system properties must be a valid json.", null)
        .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
    }
  }

  Map<String, String> getFileSystemProperties() {
    if (fileSystemProperties == null) {
      return new HashMap<>();
    }
    try {
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse filesystem properties: " + e.getMessage(), e);
    }
  }

  public String getPath() {
    return path;
  }
}
