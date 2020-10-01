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

package io.cdap.plugin.format.avro.input;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;

import java.util.Map;

/**
 * Provides and sets up configuration for an AvroInputFormat.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(AvroInputFormatProvider.NAME)
@Description(AvroInputFormatProvider.DESC)
public class AvroInputFormatProvider extends PathTrackingInputFormatProvider<PathTrackingConfig> {
  static final String NAME = "avro";
  static final String DESC = "Plugin for reading files in avro format.";
  public static final PluginClass PLUGIN_CLASS =
    new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, AvroInputFormatProvider.class.getName(),
                    "conf", PathTrackingConfig.FIELDS);

  public AvroInputFormatProvider(PathTrackingConfig conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return CombineAvroInputFormat.class.getName();
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    Schema schema = conf.getSchema();
    if (schema != null) {
      properties.put("avro.schema.input.key", schema.toString());
    }
  }
}
