/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.input.TextInputProvider;
import co.cask.hydrator.format.plugin.AbstractFileSource;
import co.cask.hydrator.format.plugin.FileSourceProperties;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} that reads from an FTP or SFTP server.
 */
@Plugin(type = "batchsource")
@Name("FTP")
@Description("Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines " +
  "the source server type, either FTP or SFTP.")
public class FTPBatchSource extends AbstractFileSource {
  private final FTPBatchSourceConfig config;

  public FTPBatchSource(FTPBatchSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    // Limit the number of splits to 1 since FTPInputStream does not support seek;
    properties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    return properties;
  }

  /**
   * Config class that contains all the properties needed for FTP Batch Source.
   */
  @SuppressWarnings("unused")
  public static class FTPBatchSourceConfig extends PluginConfig implements FileSourceProperties {
    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Macro
    @Description("Name be used to uniquely identify this source for lineage, annotating metadata, etc.")
    private String referenceName;

    @Macro
    @Description("Path to file(s) to be read. Path is expected to be of the form " +
      "'prefix://username:password@hostname:port/path'.")
    private String path;

    @Macro
    @Nullable
    @Description("Any additional properties to use when reading from the filesystem. "
      + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    private String fileSystemProperties;

    @Nullable
    @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
      + "does not exist. When true, the run will not fail and the source will not generate any output. "
      + "The default value is false.")
    private Boolean ignoreNonExistingFolders;

    @Macro
    @Nullable
    @Description("Regular expression that file names must match in order to be read. "
      + "If no value is given, no file filtering will be done. "
      + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
      + "the regular expression syntax.")
    private String fileRegex;

    @Override
    public void validate() {
      getFileSystemProperties();
    }

    @Override
    public String getReferenceName() {
      return referenceName;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public FileFormat getFormat() {
      return FileFormat.TEXT;
    }

    @Nullable
    @Override
    public Pattern getFilePattern() {
      return null;
    }

    @Override
    public long getMaxSplitSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean shouldAllowEmptyInput() {
      return false;
    }

    @Override
    public boolean shouldReadRecursively() {
      return false;
    }

    @Nullable
    @Override
    public String getPathField() {
      return null;
    }

    @Override
    public boolean useFilenameAsPath() {
      return false;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      return TextInputProvider.getDefaultSchema(null);
    }

    Map<String, String> getFileSystemProperties() {
      if (fileSystemProperties == null) {
        return new HashMap<>();
      }
      try {
        return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse filesystem properties: " + e.getMessage());
      }
    }
  }
}
