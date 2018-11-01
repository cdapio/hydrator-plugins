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

package co.cask.format.parquet;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for parquet.
 */
@Plugin(type = "outputformat")
@Name(ParquetOutputFormatProvider.NAME)
@Description(ParquetOutputFormatProvider.DESC)
public class ParquetOutputFormatProvider implements OutputFormatProvider {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  static final String SCHEMA_KEY = "parquet.avro.schema";
  static final String NAME = "parquet";
  static final String DESC = "Plugin for writing files in parquet format.";
  private static final String PARQUET_COMPRESSION = "parquet.compression";
  private final Conf conf;

  public ParquetOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredParquetOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    conf.validate();
    Map<String, String> configuration = new HashMap<>();
    if (conf.schema != null) {
      configuration.put(SCHEMA_KEY, conf.schema);
    }

    if (conf.compressionCodec != null && !"none".equalsIgnoreCase(conf.compressionCodec)) {
      try {
        CompressionCodec.valueOf(conf.compressionCodec.toUpperCase());
        configuration.put(PARQUET_COMPRESSION, conf.compressionCodec.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec, e);
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
      "Compression codec to use when writing data. Must be 'snappy', 'gzip', 'lzo', or 'none'.";

    @Macro
    @Nullable
    @Description(SCHEMA_DESC)
    private String schema;

    @Macro
    @Nullable
    @Description(CODEC_DESC)
    private String compressionCodec;

    private void validate() {
      if (!containsMacro("schema") && schema != null) {
        try {
          Schema.parseJson(schema);
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
        }
      }
    }
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", Conf.SCHEMA_DESC, "string", false, true));
    properties.put("compressionCodec",
                   new PluginPropertyField("compressionCodec", Conf.CODEC_DESC, "string", false, true));
    return new PluginClass("outputformat", NAME, DESC, ParquetOutputFormatProvider.class.getName(),
                           "conf", properties);
  }
}
