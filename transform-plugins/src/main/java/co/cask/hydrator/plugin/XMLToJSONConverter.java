/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.format.StructuredRecordStringConverter;

import java.io.IOException;

/**
 * Transform parses a JSON Object into {@link StructuredRecord}.
 */
@Plugin(type = "transform")
@Name("JSONParser")
@Description("Parses JSON Object into a Structured Record.")
public final class XMLToJSONConverter extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  // Output Schema that specifies the fileds of JSON object.
  private Schema outSchema;

  // Mainly used for testing.
  public XMLToJSONConverter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.field) == null) {
      throw new IllegalArgumentException(String.format("Field %s is not present in input schema", config.field));
    }
  }
  
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      outSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }
  
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecordStringConverter.fromJsonString((String) input.get(config.field), outSchema));
  }

  /**
   * JSONParser Plugin Config.
   */
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("Input Field")
    private String field;

    @Name("schema")
    @Description("Output schema")
    private String schema;

    public Config(String field, String schema) {
      this.field = field;
      this.schema = schema;
    }

  }
}

