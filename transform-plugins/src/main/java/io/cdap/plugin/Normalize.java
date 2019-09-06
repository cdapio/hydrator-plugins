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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms records by normalizing the data.
 * Convert wide rows and reducing data to it canonicalize form
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Normalize")
@Description("Convert wide rows and reducing data to it canonicalize form")
public class Normalize extends Transform<StructuredRecord, StructuredRecord> {
  private static final String NAME_KEY_SUFFIX = "_name";
  private static final String VALUE_KEY_SUFFIX = "_value";

  private final NormalizeConfig config;

  private Schema outputSchema;
  private Map<String, String> mappingFieldMap;
  private Map<String, String> normalizeFieldMap;
  private List<String> normalizeFieldList;

  public Normalize(NormalizeConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    outputSchema = getSchema(collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    validateFields(outputSchema, collector);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    validateMappings(inputSchema, collector);
  }

  private void initializeFieldData() {
    if (normalizeFieldList != null) {
      return;
    }

    mappingFieldMap = new HashMap<>();
    String[] fieldMappingArray = config.fieldMapping.split(",");
    for (String fieldMapping : fieldMappingArray) {
      String[] mappings = fieldMapping.split(":");
      mappingFieldMap.put(mappings[0], mappings[1]);
    }

    normalizeFieldMap = new HashMap<>();
    normalizeFieldList = new ArrayList<>();
    String[] fieldNormalizingArray = config.fieldNormalizing.split(",");

    for (String fieldNormalizing : fieldNormalizingArray) {
      String[] fields = fieldNormalizing.split(":");
      normalizeFieldList.add(fields[0]);
      normalizeFieldMap.put(fields[0] + NAME_KEY_SUFFIX, fields[1]);
      normalizeFieldMap.put(fields[0] + VALUE_KEY_SUFFIX, fields[2]);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    initializeFieldData();
    if (outputSchema != null) {
      return;
    }
    FailureCollector collector = context.getFailureCollector();
    outputSchema = getSchema(collector);
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    for (String normalizeField : normalizeFieldList) {
      if (structuredRecord.get(normalizeField) == null) {
        continue;
      }
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      String normalizeFieldValue = String.valueOf(structuredRecord.<Object>get(normalizeField));
      //Set normalize fields to the record
      builder.set(normalizeFieldMap.get(normalizeField + NAME_KEY_SUFFIX), normalizeField)
        .set(normalizeFieldMap.get(normalizeField + VALUE_KEY_SUFFIX), normalizeFieldValue);

      //Set mapping fields to the record
      mappingFieldMap.forEach((key, value) -> builder.set(value, String.valueOf(structuredRecord.<Object>get(key))));
      emitter.emit(builder.build());
    }
  }

  private Schema getSchema(FailureCollector collector) {
    try {
      return Schema.parseJson(config.outputSchema);
    } catch (IOException e) {
      collector.addFailure("Format of schema specified is invalid.", "Please check the format.")
          .withConfigProperty("schema");
      throw collector.getOrThrowException();
    }
  }

  private void validateFields(Schema outputSchema, FailureCollector collector) {
    for (Schema.Field outputField : outputSchema.getFields()) {
      Schema fieldSchema = outputField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() :
          fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        collector.addFailure("Field '" + outputField.getName() + "' is of invalid type " + fieldType + ".",
            "Please ensure that all output schema fields are of type STRING.")
            .withOutputSchemaField(outputField.getName(), null);
      }
    }
  }

  private void validateMappings(Schema inputSchema, FailureCollector collector) {
    List<String> fieldList = new ArrayList<>();
    //Validate mapping fields
    String[] fieldMappingArray = config.fieldMapping.split(",");
    for (String fieldMapping : fieldMappingArray) {
      String[] mappings = fieldMapping.split(":");
      if (mappings.length != 2) {
        collector.addFailure("Mapping field '" + mappings[0] + "' is invalid.",
            "Please specify both input and output schema fields.").withConfigProperty("fieldMapping");
        throw collector.getOrThrowException();
      }
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null && inputSchema.getField(mappings[0]) == null) {
        collector.addFailure("Mapping field '" + mappings[0] + "' is not present in the input schema.",
            "Please ensure that all mapping fields are present in the input schema.")
            .withConfigElement("fieldMapping", mappings[0]);
      }
      if (outputSchema.getField(mappings[1]) == null) {
        collector.addFailure("Output schema mapping field '" + mappings[1] + "' is not present in the output schema.",
            "Please ensure that all mapping fields are present in the output schema.")
            .withConfigElement("fieldMapping", mappings[1]);
      }
      fieldList.add(mappings[0]);
    }


    //Validate normalizing fields
    String[] fieldNormalizingArray = config.fieldNormalizing.split(",");

    //Type and Value mapping for all normalize fields must be same, otherwise it is invalid.
    //Read type and value from first normalize fields which is used for validation.
    String[] typeValueFields = fieldNormalizingArray[0].split(":");
    if (typeValueFields.length != 3) {
      collector.addFailure("Normalizing field '" + typeValueFields[0] + "' is invalid.",
          "Please specify the required Field Type and Field Value columns.")
      .withConfigElement("fieldNormalizing", fieldNormalizingArray[0]);
      throw collector.getOrThrowException();
    }
    String validTypeField = typeValueFields[1];
    String validValueField = typeValueFields[2];

    for (String fieldNormalizing : fieldNormalizingArray) {
      String[] fields = fieldNormalizing.split(":");
      if (fields.length != 3) {
        collector.addFailure("Normalizing field '" + fields[0] + "' is invalid.",
            "Please specify the required Field Type and Field Value columns.")
            .withConfigElement("fieldNormalizing", fields[0]);
        throw collector.getOrThrowException();
      }
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null && inputSchema.getField(fields[0]) == null) {
        collector.addFailure("Normalizing field '" + fields[0] + "' is not present in the input schema.",
            "Please ensure all normalizing fields are present in the input schema.")
            .withConfigElement("fieldNormalizing", fields[0]);
      }
      if (fieldList.contains(fields[0])) {
        collector.addFailure("'" + fields[0] + "' cannot be used for both mapping and normalize fields.",
            "Please use the field as either a mapping or normalize field.")
            .withConfigElement("fieldNormalizing", fields[0]).withConfigElement("fieldMapping", fields[0]);
      }
      if (!validTypeField.equals(fields[1])) {
        collector.addFailure("Type mapping is invalid for normalize field '" + fields[0] + "'.",
            "Type mapping must be the same for all normalize fields.")
            .withConfigElement("fieldNormalizing", fields[0]);
      }
      if (!validValueField.equals(fields[2])) {
        collector.addFailure("Value mapping is invalid for normalize field '" + fields[0] + "'.",
            "Value mapping must be the same for all normalize fields.")
            .withConfigElement("fieldNormalizing", fields[0]);
      }
      if (outputSchema.getField(fields[1]) == null) {
        collector.addFailure("Type mapping '" + fields[1] + "' is not present in the output schema.",
            "Please specify add the type mapping to the output schema, or remove the type mapping.")
            .withConfigProperty("outputSchema").withConfigElement("fieldNormalizing", fields[1]);
      }
      if (outputSchema.getField(fields[2]) == null) {
        collector.addFailure("Value mapping '" + fields[2] + "' is not present in the output schema.",
            "Please specify add the value mapping to the output schema, or remove the value mapping.")
            .withConfigProperty("outputSchema").withConfigElement("fieldNormalizing", fields[2]);
      }
    }
  }

  /**
   * Configuration for the Normalize transform.
   */
  public static class NormalizeConfig extends PluginConfig {
    @Description("Specify the input schema field mapping to output schema field. " +
      "Example: CustomerID:ID, here value of CustomerID will be saved to ID field of output schema.")
    private final String fieldMapping;

    @Description("Specify the normalize field name, to what output field it should be mapped to and where the value " +
      "needs to be added. Example: ItemId:AttributeType:AttributeValue, here ItemId column name will be saved to " +
      "AttributeType field and its value will be saved to AttributeValue field of output schema")
    private final String fieldNormalizing;

    @Description("The output schema for the data as it will be formatted in CDAP. Sample schema: {\n" +
      "    \"type\": \"schema\",\n" +
      "    \"name\": \"outputSchema\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\": \"id\",\n" +
      "            \"type\": \"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"type\",\n" +
      "            \"type\": \"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"value\",\n" +
      "            \"type\": \"string\"\n" +
      "        }" +
      "    ]\n" +
      "}")
    private final String outputSchema;

    public NormalizeConfig(String fieldMapping, String fieldNormalizing, String outputSchema) {
      this.fieldMapping = fieldMapping;
      this.fieldNormalizing = fieldNormalizing;
      this.outputSchema = outputSchema;
    }

    private void validate(FailureCollector collector) {
      if (Strings.isNullOrEmpty(fieldMapping)) {
        collector.addFailure("Fields to be mapped cannot be empty.", "Please populate the fields"
            + "to be mapped.").withConfigProperty("fieldMapping");
        throw collector.getOrThrowException();
      }
      if (Strings.isNullOrEmpty(fieldNormalizing)) {
        collector.addFailure("Fields to be normalized cannot be empty.", "Please populate the fields"
            + "to be normalized.").withConfigProperty("fieldNormalizing");
        throw collector.getOrThrowException();
      }
      if (Strings.isNullOrEmpty(outputSchema)) {
        collector.addFailure("Output schema cannot be empty", "Please populate the output schema.")
        .withConfigProperty("outputSchema");
        throw collector.getOrThrowException();
      }
    }
  }
}
