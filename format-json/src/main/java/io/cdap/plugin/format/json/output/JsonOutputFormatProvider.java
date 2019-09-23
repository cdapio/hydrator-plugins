/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.json.output;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.format.output.AbstractOutputFormatProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Output format plugin for json.
 */
@Plugin(type = ValidatingOutputFormat.PLUGIN_TYPE)
@Name(JsonOutputFormatProvider.NAME)
@Description(JsonOutputFormatProvider.DESC)
public class JsonOutputFormatProvider extends AbstractOutputFormatProvider {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  static final String NAME = "json";
  static final String DESC = "Plugin for writing files in json format.";

  @Override
  public String getOutputFormatClassName() {
    return StructuredJsonOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return Collections.emptyMap();
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    return new PluginClass(ValidatingOutputFormat.PLUGIN_TYPE, NAME, DESC, JsonOutputFormatProvider.class.getName(),
                           null, properties);
  }
}
