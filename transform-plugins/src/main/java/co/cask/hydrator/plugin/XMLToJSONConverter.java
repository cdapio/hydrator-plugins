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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import org.json.JSONObject;
import org.json.XML;

/**
 * Transform parses an XML String field into a stringified JSON Object.
 */
@Plugin(type = "transform")
@Name("XMLToJSON")
@Description("Converts an XML string to a JSON string")
public final class XMLToJSONConverter extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("jsonevent", Schema.of(Schema.Type.STRING))
  );

  // Mainly used for testing.
  public XMLToJSONConverter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.field) == null) {
      throw new IllegalArgumentException(String.format("Field %s is not present in input schema", config.field));
    }
  }
  
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    JSONObject jsonBody = XML.toJSONObject((String) input.get(config.field));
    StructuredRecord.Builder builder = StructuredRecord.builder(DEFAULT_SCHEMA);
    emitter.emit(builder.set("jsonevent", jsonBody.toString()).build());
  }

  /**
   * XMLToJSONConverter Plugin Config.
   */
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("The field containing the XML string to convert into a JSON string.")
    private String field;

    public Config(String field) {
      this.field = field;
    }
  }
}

