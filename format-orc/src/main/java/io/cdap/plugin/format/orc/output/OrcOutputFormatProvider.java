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

package io.cdap.plugin.format.orc.output;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.HiveSchemaConverter;
import io.cdap.plugin.format.output.AbstractOutputFormatProvider;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Output format plugin for ORC.
 */
@Plugin(type = ValidatingOutputFormat.PLUGIN_TYPE)
@Name(OrcOutputFormatProvider.NAME)
@Description(OrcOutputFormatProvider.DESC)
public class OrcOutputFormatProvider extends AbstractOutputFormatProvider {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  static final String NAME = "orc";
  static final String DESC = "Plugin for writing files in orc format.";
  private static final String ORC_COMPRESS = "orc.compress";
  private static final String SNAPPY_CODEC = "SNAPPY";
  private static final String ZLIB_CODEC = "ZLIB";
  private static final String COMPRESS_SIZE = "orc.compress.size";
  private static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  private static final String CREATE_INDEX = "orc.create.index";
  private final Conf conf;

  public OrcOutputFormatProvider(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String getOutputFormatClassName() {
    return StructuredOrcOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> configuration = new HashMap<>();
    configuration.put("orc.mapred.output.schema", parseOrcSchema(conf.schema));

    if (conf.compressionCodec != null && !conf.compressionCodec.equalsIgnoreCase("None")) {
      try {
        CompressionKind.valueOf(conf.compressionCodec.toUpperCase());
        configuration.put(ORC_COMPRESS, conf.compressionCodec.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported compression codec " + conf.compressionCodec);
      }

      if (conf.compressionChunkSize != null) {
        configuration.put(COMPRESS_SIZE, String.valueOf(conf.compressionChunkSize));
      }
      if (conf.stripeSize != null) {
        configuration.put(COMPRESS_SIZE, String.valueOf(conf.stripeSize.toString()));
      }
      if (conf.indexStride != null) {
        configuration.put(ROW_INDEX_STRIDE, String.valueOf(conf.indexStride));
      }
      if (conf.createIndex != null) {
        configuration.put(CREATE_INDEX, String.valueOf(conf.createIndex));
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
      "Compression codec to use when writing data. Must be 'snappy', 'zlib', or 'none'.";
    private static final String CHUNK_DESC = "Number of bytes in each compression chunk.";
    private static final String STRIPE_SIZE_DESC = "Number of bytes in each stripe.";
    private static final String INDEX_STRIDE_DESC =
      "Number of rows between index entries. The value must be at least 1000.";
    private static final String INDEX_CREATE_DESC = "Whether to create inline indexes.";

    @Macro
    @Description(SCHEMA_DESC)
    private String schema;

    @Macro
    @Nullable
    @Description(CODEC_DESC)
    private String compressionCodec;

    @Macro
    @Nullable
    @Description(CHUNK_DESC)
    private Long compressionChunkSize;

    @Macro
    @Nullable
    @Description(STRIPE_SIZE_DESC)
    private Long stripeSize;

    @Macro
    @Nullable
    @Description(INDEX_STRIDE_DESC)
    private Long indexStride;

    @Macro
    @Nullable
    @Description(INDEX_CREATE_DESC)
    private Boolean createIndex;
  }

  private static String parseOrcSchema(String configuredSchema) {
    try {
      io.cdap.cdap.api.data.schema.Schema schemaObj = io.cdap.cdap.api.data.schema.Schema.parseJson(configuredSchema);
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, schemaObj);
      return builder.toString();
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("%s is not a valid schema", configuredSchema), e);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(String.format("Could not create hive schema from %s", configuredSchema), e);
    }
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", Conf.SCHEMA_DESC, "string", true, true));
    properties.put("compressionCodec",
                   new PluginPropertyField("compressionCodec", Conf.CODEC_DESC, "string", false, true));
    properties.put("compressionChunkSize",
                   new PluginPropertyField("compressionChunkSize", Conf.CHUNK_DESC, "long", false, true));
    properties.put("stripeSize", new PluginPropertyField("stripeSize", Conf.STRIPE_SIZE_DESC, "long", false, true));
    properties.put("indexStride", new PluginPropertyField("indexStride", Conf.INDEX_STRIDE_DESC, "long", false, true));
    properties.put("createIndex",
                   new PluginPropertyField("createIndex", Conf.INDEX_CREATE_DESC, "boolean", false, true));
    return new PluginClass(ValidatingOutputFormat.PLUGIN_TYPE, NAME, DESC, OrcOutputFormatProvider.class.getName(),
                           "conf", properties);
  }
}
