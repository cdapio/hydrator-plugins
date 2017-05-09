/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "batchsource")
@Name("File")
@Description("Batch source for File Systems")
public class FileBatchSource extends AbstractFileBatchSource {
  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come" +
    " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where" +
    " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'. The path uses " +
    "filename expansion (globbing) to read files.";

  private final FileBatchConfig config;

  public FileBatchSource(FileBatchConfig config) {
    super(config);
    this.config = config;
  }

  @VisibleForTesting
  FileBatchConfig getConfig() {
    return config;
  }

  /**
   * Config class that contains all the properties needed for the file source.
   */
  public static class FileBatchConfig extends FileSourceConfig {
    @Description(PATH_DESCRIPTION)
    @Macro
    public String path;

    @VisibleForTesting
    public FileBatchConfig() {
      this(null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public FileBatchConfig(String referenceName, @Nullable String fileRegex, @Nullable String timeTable,
                           @Nullable String inputFormatClass, @Nullable String fileSystemProperties,
                           @Nullable Long maxSplitSize, @Nullable Boolean ignoreNonExistingFolders,
                           @Nullable Boolean recursive, String path, @Nullable String pathField,
                           @Nullable Boolean fileNameOnly, @Nullable String schema) {
      super(referenceName, fileRegex, timeTable, inputFormatClass, fileSystemProperties, maxSplitSize,
            ignoreNonExistingFolders, recursive, pathField, fileNameOnly, schema);
      this.path = path;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
