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

/**
 * Given a field and a delimiter, this splits it into multiple records.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("RecordSplitter")
@Description("Given a field and a delimiter, this splits it into multiple records.")
public final class RecordSplitter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordSplitter.class);
  private final Config config;

  // Output Schema associated with transform output.
  private Schema outSchema;

  // Required only for testing.
  public RecordSplitter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    // Just checking that the operator is valid
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.", e);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      config.validate();
      outSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.", e);
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    List<Schema.Field> fields = in.getSchema().getFields();
    String[] records = String.valueOf(in.get(config.fieldToSplit)).split(config.delimiter);
    for (String record : records) {
      StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
      for (Schema.Field field : fields) {
        String name = field.getName();
        if (outSchema.getField(name) != null && !name.equals(config.fieldToSplit)) {
          builder.set(name, in.get(name));
        }
      }
      builder.set(config.outputField, record.trim());
      emitter.emit(builder.build());
    }
  }

  /**
   * Record Splitter plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("fieldToSplit")
    @Description("Specifies the field to split.")
    @Macro
    private final String fieldToSplit;

    @Name("delimiter")
    @Description("Specifies the delimiter used to split each record. If using escape characters, " +
                 "be sure to double escape. So \n = \\n")
    @Macro
    private final String delimiter;

    @Name("outputField")
    @Description("Specifies the name of the output field.")
    @Macro
    private final String outputField;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String fieldToSplit, String delimiter, String outputField, String schema) {
      this.fieldToSplit = fieldToSplit;
      this.delimiter = delimiter;
      this.outputField = outputField;
      this.schema = schema;
    }

    public void validate() throws IllegalArgumentException {
      try {
        Schema outSchema = Schema.parseJson(schema);
        if (outSchema.getField(outputField) == null) {
          throw new IllegalArgumentException("Output Schema must contain the specified outputField.");
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.", e);
      }
    }
  }
}
