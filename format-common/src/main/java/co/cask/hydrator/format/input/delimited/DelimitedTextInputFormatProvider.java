/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input.delimited;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.hydrator.format.input.InputFormatConfig;
import co.cask.hydrator.format.input.PathTrackingInputFormatProvider;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Reads delimited text into StructuredRecords.
 */
@Plugin(type = "inputformat")
@Name("delimited")
@Description("Delimited text input format plugin for file based plugins.")
public class DelimitedTextInputFormatProvider extends
  PathTrackingInputFormatProvider<DelimitedTextInputFormatProvider.DelimitedConf> {
  private final DelimitedConf conf;

  public DelimitedTextInputFormatProvider(DelimitedConf conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public String getInputFormatClassName() {
    return CombineDelimitedInputFormat.class.getName();
  }

  @Override
  protected void validate() {
    if (conf.getSchema() == null) {
      throw new IllegalArgumentException("Delimited format cannot be used without specifying a schema.");
    }
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    properties.put(PathTrackingDelimitedInputFormat.DELIMITER, conf.delimiter == null ? "," : conf.delimiter);
  }

  /**
   * Plugin config for delimited input format
   */
  public static class DelimitedConf extends InputFormatConfig {
    @Macro
    @Nullable
    @Description("Delimiter that separates each field.")
    private String delimiter;
  }
}
