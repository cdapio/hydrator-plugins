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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Adds text to the beginning or end of a set of fields.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("PrefixSuffix")
@Description("Adds text to the beginning or end of a field.")
public final class PrefixSuffix extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixSuffix.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;
  private Map<String, Map<String, String>> toPrefixSuffix;

  // Required only for testing.
  public PrefixSuffix(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    // Just checking that the operator is valid
    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      config.validate();
      outSchema = Schema.parseJson(config.schema);
      toPrefixSuffix = config.parseFieldsToPrefixSuffix();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    List<Schema.Field> fields = in.getSchema().getFields();
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (outSchema.getField(name) != null) {
        StringBuilder sb = new StringBuilder(String.valueOf(in.get(name)).trim());
        if (toPrefixSuffix.get(Config.PREFIX_KEY).get(name) != null) {
          sb.insert(0, toPrefixSuffix.get(Config.PREFIX_KEY).get(name));
        }
        if (toPrefixSuffix.get(Config.SUFFIX_KEY).get(name) != null) {
          sb.append(toPrefixSuffix.get(Config.SUFFIX_KEY).get(name));
        }
        builder.convertAndSet(name, sb.toString());
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Prefix Suffix plugin configuration.
   */
  public static class Config extends PluginConfig {
    private static final String PREFIX_KEY = "prefix";
    private static final String SUFFIX_KEY = "suffix";
    private static final String DELIMITER = "\n";
    private static final String KV_DELIMITER = "\t";

    @Name("fieldsToPrefix")
    @Description("Specifies the fields and values to append.")
    @Nullable
    @Macro
    private final String fieldsToPrefix;

    @Name("fieldsToSuffix")
    @Description("Specifies the fields and values to append.")
    @Nullable
    @Macro
    private final String fieldsToSuffix;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String fieldsToPrefix, String fieldsToSuffix, String schema) {
      this.fieldsToPrefix = fieldsToPrefix;
      this.fieldsToSuffix = fieldsToSuffix;
      this.schema = schema;
    }

    public Map<String, Map<String, String>> parseFieldsToPrefixSuffix() {
      Map<String, Map<String, String>> returnValue = new HashMap<>();
      try {
        if (Strings.isNullOrEmpty(fieldsToPrefix)) {
          returnValue.put(PREFIX_KEY, new HashMap<String, String>());
        } else {
          List<String> fields = Arrays.asList(fieldsToPrefix.split(DELIMITER));
          Map<String, String> toPrefix = Maps.newHashMap();
          for (String field : fields) {
            String[] keyValue = field.split(KV_DELIMITER);
            toPrefix.put(keyValue[0], keyValue[1]);
          }
          returnValue.put(PREFIX_KEY, toPrefix);
        }
        if (Strings.isNullOrEmpty(fieldsToSuffix)) {
          returnValue.put(SUFFIX_KEY, new HashMap<String, String>());
        } else {
          List<String> fields = Arrays.asList(fieldsToSuffix.split(DELIMITER));
          Map<String, String> toSuffix = Maps.newHashMap();
          for (String field : fields) {
            String[] keyValue = field.split(KV_DELIMITER);
            toSuffix.put(keyValue[0], keyValue[1]);
          }
          returnValue.put(SUFFIX_KEY, toSuffix);
        }
        return returnValue;
      } catch (Exception e) {
        throw new IllegalArgumentException("Was not able to parse fields to Prefix or Suffix.");
      }
    }

    public void validate() throws IllegalArgumentException {
      // Check to make sure fields in fieldsToAppend are in output schema
      Map<String, Map<String, String>> toPrefixSuffix = parseFieldsToPrefixSuffix();
      try {
        Schema outSchema = Schema.parseJson(schema);
        for (Map.Entry<String, Map<String, String>> keyValue : toPrefixSuffix.entrySet()) {
          for (String key : keyValue.getValue().keySet()) {
            if (outSchema.getField(key) == null) {
              throw new IllegalArgumentException("Output Schema must contain all fields to prefix or suffix.");
            }
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }
  }
}
