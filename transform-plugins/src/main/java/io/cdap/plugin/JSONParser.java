/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.cdap.cdap.api.annotation.Description;
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
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import scala.util.Failure;

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
  private Map<String, String> mapping = Maps.newHashMap();

  private List<Schema.Field> fields;

  // Specifies whether mapping is simple or complex.
  private boolean isSimple = true;

  // Mainly used for testing.
  public JSONParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema outputSchema = getSchema(collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    fields = outputSchema.getFields();

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null && inputSchema.getField(config.field) == null) {
      collector.addFailure("Field " + config.field + " is not present in the input schema",
          "Please ensure that the input field is present in the input schema.")
          .withConfigProperty("field");
    }
    extractMappings(collector);
  }

  // If there is no config mapping, then we attempt to directly map output schema fields
  // to JSON directly, but, if there is a mapping specified, then we take the mapping to
  // populate the output schema fields.
  //
  // E.g. expensive:$.expensive maps the input Json path from root, field expensive to expensive.
  private void extractMappings(FailureCollector collector) {
    if (config.mapping == null || config.mapping.isEmpty()) {
      isSimple = true;
    } else {
      isSimple = false;
      String[] pathMaps = config.mapping.split(",");
      for (String pathMap : pathMaps) {
        String[] mapParts = pathMap.split(":");
        String field = mapParts[0];
        String expression = mapParts[1];
        if (field.isEmpty() && !expression.isEmpty()) {
          collector.addFailure("JSON path expression '" + expression + "' has no output field specified.",
              "Please specify an output field for the JSON path expression.")
              .withConfigProperty("mapping");
        }
        if (expression.isEmpty() && !field.isEmpty()) {
          collector.addFailure("Field '" + field + "' doesn't have a JSON path expression.",
              "Please specify a JSON path expression.")
              .withConfigProperty("mapping");
        }
        mapping.put(field, expression);
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    outSchema = getSchema(collector);
    fields = outSchema.getFields();
    extractMappings(collector);
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
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (mapping.containsKey(name)) {
        String path = mapping.get(name);
        try {
          Object value = JsonPath.read(document, path);
          builder.set(field.getName(), value);
        } catch (PathNotFoundException e) {
          if (field.getSchema().isNullable()) {
            builder.set(field.getName(), null);
          } else {
            LOG.error("Json path '" + path + "' specified for the field '" + name + "' doesn't exist. " +
                        "Dropping the error record: " + StructuredRecordStringConverter.toJsonString(input));
            return;
          }
        }
      } else {
        // We didn't find the field name in the mapping, we will not attempt to see if the field is present
        // in the input; if it is, then we will transfer the input field value to the output field value.
        Object value = input.get(name);
        if (value != null) {
          builder.set(name, value);
        }
      }
    }
    emitter.emit(builder.build());
  }

  private Schema getSchema(FailureCollector collector) {
    try {
      return Schema.parseJson(config.schema);
    } catch (IOException e) {
      collector.addFailure("Format of schema specified is not a valid JSON.",
          "Please check the schema JSON.").withConfigProperty("schema");
      throw collector.getOrThrowException();
    }
  }

  /**
   * JSONParser Plugin Config.
   */
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("Input field to be parsed as JSON")
    private String field;

    @Name("mapping")
    @Description("Maps complex JSON to output fields using JSON path expressions. First field defines the output " +
      "field name and the second field specifies the JSON path expression, such as '$.employee.name.first'. " +
      "See reference documentation for additional examples.")
    @Nullable
    private String mapping;

    @Name("schema")
    @Description("Output schema")
    private String schema;

    public Config(String field, @Nullable String mapping, String schema) {
      this.field = field;
      this.mapping = mapping;
      this.schema = schema;
    }

  }
}
