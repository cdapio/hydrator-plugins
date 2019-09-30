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

package io.cdap.plugin.format.avro.output;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.format.output.AbstractOutputFormatProvider;
import org.apache.avro.file.CodecFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for avro.
 */
@Plugin(type = ValidatingOutputFormat.PLUGIN_TYPE)
@Name(AvroOutputFormatProvider.NAME)
@Description(AvroOutputFormatProvider.DESC)
public class AvroOutputFormatProvider extends AbstractOutputFormatProvider {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  static final String SCHEMA_KEY = "avro.schema.output.key";
  static final String NAME = "avro";
  static final String DESC = "Plugin for writing files in avro format.";
  private static final String AVRO_OUTPUT_CODEC = "avro.output.codec";
  private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
  private final Conf conf;

  public AvroOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredAvroOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> configuration = new HashMap<>();
    if (!conf.containsMacro("schema")) {
      configuration.put(SCHEMA_KEY, conf.schema);
    }

    if (conf.compressionCodec != null && !conf.containsMacro("compressionCodec") &&
      !"none".equalsIgnoreCase(conf.compressionCodec)) {

      try {
        CodecFactory.fromString(conf.compressionCodec.toLowerCase());
        configuration.put(MAPRED_OUTPUT_COMPRESS, "true");
        configuration.put(AVRO_OUTPUT_CODEC, conf.compressionCodec.toLowerCase());
      } catch (Exception e) {
        throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec);
      }
    }
    return configuration;
  }

  /**
   * Configuration for the output format plugin.
   */
  public static class Conf extends PluginConfig {
    private static final String SCHEMA_DESC = "Schema of the data to write.";
    private static final String CODEC_DESC =
      "Compression codec to use when writing data. Must be 'snappy', 'deflate', 'bzip2', 'xz', or 'none.'";

    @Macro
    @Description(SCHEMA_DESC)
    private String schema;

    @Macro
    @Nullable
    @Description(CODEC_DESC)
    private String compressionCodec;
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", Conf.SCHEMA_DESC, "string", true, true));
    properties.put("compressionCodec",
                   new PluginPropertyField("compressionCodec", Conf.CODEC_DESC, "string", false, true));
    return new PluginClass(ValidatingOutputFormat.PLUGIN_TYPE, NAME, DESC, AvroOutputFormatProvider.class.getName(),
                           "conf", properties);
  }
}
