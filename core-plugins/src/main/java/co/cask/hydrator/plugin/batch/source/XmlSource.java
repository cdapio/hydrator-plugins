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
import co.cask.hydrator.plugin.common.TagDelimitedInputFormat;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

/**
 * An XML Source plugin that reads a split XML file(s). 
 * This plugin requires one to describe the start and end tag of XML that defines 
 * a record. It uses the {@link TagDelimitedInputFormat} to read the record defined
 * between start tag and end tag.  
 */
@Plugin(type = "batchsource")
@Name("Xml")
@Description("Reads XML file(s) and returns XML record based on start and end tags.")
public final class XmlSource extends FileSource {
  // XML Configuration extended from File Source config.
  private final XmlConfig config;

  /**
   * Constructor for XmlSource
   * @param config Plugin configuration.
   */
  public XmlSource(XmlConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Sets the right configurations required for {@link TagDelimitedInputFormat}.
   * @param configuration Hadoop configuration instance. 
   */
  @Override
  protected void setFileInputFormatProperties(Configuration configuration) {
    configuration.set(TagDelimitedInputFormat.START_TAG_KEY, config.startTag);
    configuration.set(TagDelimitedInputFormat.END_TAG_KEY, config.endTag);
  }

  /**
   * Returns a instance of {@link SourceInputFormatProvider} that knows of type {@link TagDelimitedInputFormat}
   * @param configuration Hadoop configuration instance.
   * @return {@link TagDelimitedInputFormat}
   */
  @Override
  protected SourceInputFormatProvider setInputFormatProvider(Configuration configuration) {
    return new SourceInputFormatProvider(TagDelimitedInputFormat.class.getName(), configuration);
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
