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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.hydrator.format.input.InputFormatConfig;
import co.cask.hydrator.format.input.PathTrackingInputFormatProvider;

import java.util.Map;

/**
 * Reads delimited text into StructuredRecords.
 */
@Plugin(type = "inputformat")
@Name("tsv")
@Description("TSV text input format plugin for file based plugins.")
public class TSVInputFormatProvider extends
  PathTrackingInputFormatProvider<InputFormatConfig> {

  public TSVInputFormatProvider(InputFormatConfig conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return CombineDelimitedInputFormat.class.getName();
  }

  @Override
  protected void validate() {
    if (conf.getSchema() == null) {
      throw new IllegalArgumentException("TSV format cannot be used without specifying a schema.");
    }
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    properties.put(PathTrackingDelimitedInputFormat.DELIMITER, "\t");
  }
}
