/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.util.List;

/**
 * A transform that parses an XML String field into a stringified JSON Object.
 */
@Plugin(type = "transform")
@Name("XMLToJSON")
@Description("Converts an XML string to a JSON string")
public final class XMLToJSON extends Transform<StructuredRecord, StructuredRecord> {
  private static final String INPUT_FIELD = "inputField";

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
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      Schema.Field field = inputSchema.getField(config.inputField);
      if (field == null) {
        collector.addFailure(String.format("Field '%s' must be present in input schema.", config.inputField), null)
          .withConfigProperty(INPUT_FIELD);
      } else if (field.getSchema().getType() != Schema.Type.STRING) {
        collector.addFailure(String.format("Field '%s' must be of type string.", config.inputField), null)
          .withConfigProperty(INPUT_FIELD).withInputSchemaField(config.inputField);
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
    @Description("Specifies the output field where the JSON string will be stored. "
        + "If it is not present in the output schema, it will be added.")
    @Macro
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

