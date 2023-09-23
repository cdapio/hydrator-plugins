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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
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
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    if (!config.containsMacro(Config.SCHEMA)) {
      try {
        Schema outputSchema = Schema.parseJson(config.schema);
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
        fields = outputSchema.getFields();
      } catch (IOException e) {
        collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.");
      }
    }

    if (!config.containsMacro(Config.FIELD)) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
      if (inputSchema != null && inputSchema.getField(config.field) == null) {
        collector.addFailure(String.format("Field '%s' must be present in the input schema.", config.field), null)
                .withConfigProperty(Config.FIELD);
      }
    }
    extractMappings(collector);
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    extractMappings(context.getFailureCollector());

    if (isSimple) {
      context.record(TransformLineageRecorderUtils.generateOneToMany(config.field, TransformLineageRecorderUtils
        .getFields(context.getOutputSchema()), "Parse", "Parsed fields as JSON."));
      return;
    }

    // After extracting the mappings, store a list of operations containing identity transforms for every output
    // field also present in the mappings list. No fields are dropped.
    List<String> mappedFields = TransformLineageRecorderUtils.getFields(context.getOutputSchema()).stream()
      .filter(mapping::containsKey).collect(Collectors.toList());

    List<String> identityFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    identityFields.removeAll(mappedFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(mappedFields, "Parse",
      "Parsed fields as JSON."));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
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
        if (mapParts.length != 2 || Strings.isNullOrEmpty(mapParts[0]) || Strings.isNullOrEmpty(mapParts[1])) {
          collector.addFailure("Both field name and JSON expression map must be provided.", null)
            .withConfigElement(Config.MAPPING, pathMap);
        } else {
          mapping.put(mapParts[0], mapParts[1]);
        }
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = getContext().getFailureCollector();
    try {
      outSchema = Schema.parseJson(config.schema);
      fields = outSchema.getFields();
    } catch (IOException e) {
      collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.");
      throw collector.getOrThrowException();
    }
    extractMappings(collector);
    collector.getOrThrowException();
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

  /**
   * JSONParser Plugin Config.
   */
  public static class Config extends PluginConfig {
    public static final String FIELD = "field";
    public static final String MAPPING = "mapping";
    public static final String SCHEMA = "schema";

    @Name("field")
    @Description("Input field to be parsed as JSON")
    @Macro
    private String field;

    @Name("mapping")
    @Description("Maps complex JSON to output fields using JSON path expressions. First field defines the output " +
      "field name and the second field specifies the JSON path expression, such as '$.employee.name.first'. " +
      "See reference documentation for additional examples.")
    @Nullable
    @Macro
    private String mapping;

    @Name("schema")
    @Description("Output schema")
    @Macro
    private String schema;

    public Config(String field, @Nullable String mapping, String schema) {
      this.field = field;
      this.mapping = mapping;
      this.schema = schema;
    }

  }
}
