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

import javax.annotation.Nullable;


/**
 * Fixed Length Record Source.  
 */
@Plugin(type = "batchsource")
@Name("FixedLength")
@Description("Parses Fixed Length Files")
public class FixedLengthSource extends FileSource {
  private FixedLengthConfig config;

  public FixedLengthSource(FixedLengthConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected void setFileInputFormatProperties(Configuration configuration) {
    configuration.set(FixedLengthInputFormat.FIXED_RECORD_LENGTH, Long.toString(config.recordLength));
  }

  @Override
  protected SourceInputFormatProvider setInputFormatProvider(Configuration configuration) {
    return new SourceInputFormatProvider("org.apache.hadoop.mapred.FixedLengthInputFormat", configuration);
  }

  /**
   * Fixed Length Source config.
   */
  public class FixedLengthConfig extends FileSource.FileSourceConfig {
    @Description("Specifies the length of the record")
    public Long recordLength;

    public FixedLengthConfig(String paths, @Nullable Long maxSplitSize, @Nullable String pattern,
                             Long recordLength) {
      super(paths, maxSplitSize, pattern);
      this.recordLength = recordLength;
    }
  }
}
