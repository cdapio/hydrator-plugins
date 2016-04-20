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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.hydrator.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import javax.annotation.Nullable;


/**
 * Key Value Record Source.
 */
@Plugin(type = "batchsource")
@Name("KVFile")
@Description("Files are broken into lines. Either line feed or carriage-return are used to signal end of line. " +
  "Each line is divided into key and value parts by a separator byte. If no such a byte exists, the key will be the " +
  "entire line and value will be empty.")
public class KeyValueSource extends FileSource {
  private FixedLengthConfig config;

  public KeyValueSource(FixedLengthConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void setFileInputFormatProperties(Configuration configuration) {
    configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", config.separator);
  }

  @Override
  protected SourceInputFormatProvider setInputFormatProvider(Configuration configuration) {
    return new SourceInputFormatProvider(KeyValueTextInputFormat.class.getName(), configuration);
  }

  /**
   * Key Value Source config.
   */
  public class FixedLengthConfig extends FileSourceConfig {
    @Name("key.value.separator")
    @Description("Specifies separator for key value pair.")
    public String separator;

    public FixedLengthConfig(String paths, @Nullable Long maxSplitSize, @Nullable String pattern,
                             String separator) {
      super(paths, maxSplitSize, pattern);
      this.separator = separator;
    }
  }
}
