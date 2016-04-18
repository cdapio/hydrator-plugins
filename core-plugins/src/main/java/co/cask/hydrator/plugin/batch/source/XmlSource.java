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
import co.cask.hydrator.plugin.common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

/**
 * Xml Source.
 */
@Plugin(type = "batchsource")
@Name("Xml")
@Description("Reads XML File")
public class XmlSource extends FileSource {
  private final XmlConfig config;

  public XmlSource(XmlConfig config) {
    super(config);
    this.config = config;
  }
  
  @Override
  protected void setFileInputFormatProperties(Configuration configuration) {
    configuration.set(XmlInputFormat.START_TAG_KEY, config.startTag);
    configuration.set(XmlInputFormat.END_TAG_KEY, config.endTag);
  }

  @Override
  protected SourceInputFormatProvider setInputFormatProvider(Configuration configuration) {
    return new SourceInputFormatProvider(XmlInputFormat.class.getName(), configuration);
  }

  /**
   * Xml Source configuration.
   */
  public class XmlConfig extends FileSource.FileSourceConfig {
    @Description("Specifies the start tag of XML document to be parsed as start of record.")
    public String startTag;
    
    @Description("Specifies the start tag of XML document to be parsed as end of record.")
    public String endTag;

    public XmlConfig(String paths, @Nullable Long maxSplitSize, @Nullable String pattern,
                     String startTag, String endTag) {
      super(paths, maxSplitSize, pattern);
      this.startTag = startTag;
      this.endTag = endTag;
    }
  }
}
