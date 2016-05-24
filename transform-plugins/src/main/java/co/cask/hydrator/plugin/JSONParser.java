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
import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transform parses a JSON Object into {@link StructuredRecord}.
 */
@Plugin(type = "transform")
@Name("JSONParser")
@Description("Parses JSON Object into a Structured Record.")
public final class JSONParser extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(JSONParser.class);

  private final Config config;

  // Output Schema that specifies the fileds of JSON object. 
  private Schema outSchema;

  // Map of field name to path as specified in the configuration, if none specified then it's direct mapping.
  private final Map<String, String> mapping = Maps.newHashMap();

  private List<Schema.Field> fields;

  // Specifies whether mapping is simple or complex.
  private boolean isSimple;

  // Mainly used for testing.
  public JSONParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    try {
      Schema outputSchema = Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
      fields = outputSchema.getFields();
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
      // If there is no config mapping, then we attempt to directly map output schema fields
      // to JSON directly, but, if there is a mapping specified, then we take the mapping to
      // populate the output schema fields.
      // E.g. expensive:$.expensive maps the input Json path from root, field expensive to expensive.
      if (config.mapping == null || config.mapping.isEmpty()) {
        isSimple = true;
      } else {
        isSimple = false;
        String[] pathMaps = config.mapping.split(",");
        for (String pathMap : pathMaps) {
          String[] fieldAndPath = pathMap.split(":");
          mapping.put(fieldAndPath[0], fieldAndPath[1]);
        }
      }
      } catch (IOException e) {
      throw new IllegalArgumentException("Output Schema specified is not a valid JSON. Please check the Schema JSON");
    }
  }
  
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // If it's a simple mapping from JSON to output schema, else we use the mapping fields to map the
    // the JSON using JSON path to fields. This is used for mapping complex JSON schemas.
    if (isSimple) {
      emitter.emit(StructuredRecordStringConverter.fromJsonString((String) input.get(config.field), outSchema));
      return;
    }

    // When it's not a simple Json to be parsed, we use the Json path to map the input Json fields into the
    // output schema. In order to optimize for reading multiple paths from the Json we create a document that
    // allows the Json to be parsed only once. We then iterate through the output fields and apply the
    // path to extract the fields.
    Object document = Configuration.defaultConfiguration().jsonProvider().parse((String) input.get(config.field));
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);

    for (Schema.Field field : outSchema.getFields()) {
      if (mapping.containsKey(field.getName())) {
        String name = field.getName();
        String path = mapping.get(name);
        try {
          Object value = JsonPath.read(document, path);
          builder.set(field.getName(), value.toString());
        } catch (PathNotFoundException e) {
          // if path is not found setting null. This is a valid use-case, not every field need to be present
          LOG.trace("Json path '" + path + "' specified for the field '" + name + "' doesn't exist. " +
                      "not setting this field.");

        }

      } else {
        LOG.debug("Missing mapping for field {}", field.getName());
      }

    }
    emitter.emit(builder.build());
  }

  /**
   * JSONParser Plugin Config.
   */
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("Input Field")
    private String field;

    @Name("mapping")
    @Description("Mapping complex JSON to fields using JSON Path expressions")
    private String mapping;

    @Name("schema")
    @Description("Output schema")
    private String schema;

    public Config(String field, String mapping, String schema) {
      this.field = field;
      this.mapping = mapping;
      this.schema = schema;
    }

  }
}

