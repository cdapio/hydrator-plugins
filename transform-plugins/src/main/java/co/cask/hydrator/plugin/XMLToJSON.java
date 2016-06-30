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
import co.cask.cdap.etl.api.TransformContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A transform that parses an XML String field into a stringified JSON Object.
 */
@Plugin(type = "transform")
@Name("XMLToJSON")
@Description("Converts an XML string to a JSON string")
public final class XMLToJSON extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("json_str", Schema.of(Schema.Type.STRING))
  );
  private Schema outputSchema;


  // Used only for testing.
  public XMLToJSON(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    try {
      outputSchema = Schema.parseJson(config.schema);
      if (outputSchema != null) {
        if (outputSchema.getField(config.outputField) == null) {
          // Add the output field
          outputSchema = Schema.unionOf(outputSchema, DEFAULT_SCHEMA);
        } else if (outputSchema.getField(config.outputField).getSchema().getType() != Schema.Type.STRING) {
          throw new IllegalArgumentException(String.format("Output Schema field: %s type must be a String.",
                                                           config.outputField));
        }
      } else {
        outputSchema = DEFAULT_SCHEMA;
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      if (inputSchema.getField(config.inputField) == null) {
        throw new IllegalArgumentException(String.format("Field %s is not present in input schema", config.inputField));
      }
      if (inputSchema.getField(config.inputField).getSchema().getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(String.format("Field %s must be of type string.", config.inputField));
      }
    }
  }
  
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    JSONObject jsonBody;
    try {
      jsonBody = XML.toJSONObject((String) input.get(config.inputField));
    } catch (JSONException e) {
      throw new Exception(String.format("Failed to convert XML to JSON. XML In: '%s'",
                                        (String) input.get(config.inputField)), e);
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    Schema inSchema = input.getSchema();
    List<Schema.Field> inFields = inSchema.getFields();

    for (Schema.Field field : inFields) {
      String name = field.getName();

      if (outputSchema.getField(name) != null) {
        if (name.equals(config.inputField)) {
          // skip the input field containing the xml string
          continue;
        }
        builder.set(name, input.get(name));
      }
    }
    // Finally add the transformed JSON string
    builder.set(outputSchema.getField(config.outputField).getName(), jsonBody.toString());
    emitter.emit(builder.build());
  }

  /**
   * XMLToJSON Plugin Config.
   */
  public static class Config extends PluginConfig {
    @Name("inputField")
    @Description("The field containing the XML string to be converted into a JSON string. This field will be " +
                 "dropped from the output schema.")
    private String inputField;

    @Name("outputField")
    @Description("The field containing the XML string to convert into a JSON string.")
    private String outputField;

    @Name("schema")
    @Description("Output schema")
    private String schema;

    public Config(String inputField, String outputField) {
      this.inputField = inputField;
      this.outputField = outputField;
      this.schema = DEFAULT_SCHEMA.toString();
    }

    public Config(String inputField, String outputField, String schema) {
      this.inputField = inputField;
      this.outputField = outputField;
      this.schema = schema;
    }
  }
}

