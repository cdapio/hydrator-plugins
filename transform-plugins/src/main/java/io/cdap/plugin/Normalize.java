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
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // Get failure collector for updated validation API
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    try {
      outputSchema = Schema.parseJson(config.outputSchema);
      for (Schema.Field outputField : outputSchema.getFields()) {
        Schema fieldSchema = outputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() :
          fieldSchema.getType();
        if (!fieldType.equals(Schema.Type.STRING)) {
          collector.addFailure(String.format("Invalid type '%s' for output schema field '%s'.",
                                             fieldType.toString(), outputField.getName()),
                               "All output schema fields must be of type string.")
            .withOutputSchemaField(outputField.getName());
        }
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (Exception e) {
      collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.")
        .withConfigProperty(NormalizeConfig.OUTPUT_SCHEMA);
      return;
    }

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    List<String> fieldList = new ArrayList<>();
    //Validate mapping fields
    String[] fieldMappingArray = config.fieldMapping.split(",");
    for (String fieldMapping : fieldMappingArray) {
      String[] mappings = fieldMapping.split(":");
      if (mappings.length != 2) {
        collector.addFailure("Field name and mapping must be provided.", null)
          .withConfigElement(NormalizeConfig.FIELD_MAPPING, fieldMapping);
        continue;
      }
      boolean failed = false;
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null && inputSchema.getField(mappings[0]) == null) {
        collector.addFailure(String.format("Field '%s' must be present in input schema.", mappings[0]), null)
          .withConfigElement(NormalizeConfig.FIELD_MAPPING, fieldMapping);
        failed = true;
      }
      if (outputSchema.getField(mappings[1]) == null) {
        collector.addFailure(String.format("Mapping '%s' must be present in output schema.", mappings[1]), null)
          .withConfigElement(NormalizeConfig.FIELD_MAPPING, fieldMapping);
        failed = true;
      }
      if (!failed) {
        fieldList.add(mappings[0]);
      }
    }

    //Validate normalizing fields
    String[] fieldNormalizingArray = config.fieldNormalizing.split(",");

    //Type and Value mapping for all normalize fields must be same, otherwise it is invalid.
    //Read type and value from first normalize fields which is used for validation.
    String validTypeField = null;
    String validValueField = null;

    for (String fieldNormalizing : fieldNormalizingArray) {
      String[] fields = fieldNormalizing.split(":");
      if (fields.length != 3 || Strings.isNullOrEmpty(fields[0]) ||
        Strings.isNullOrEmpty(fields[1]) || Strings.isNullOrEmpty(fields[2])) {
        collector.addFailure("Normalizing field, field name, and field value must all be provided.", null)
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
        continue;
      }
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null && inputSchema.getField(fields[0]) == null) {
        collector.addFailure(
          String.format("Normalizing field '%s' must be present in input schema.", fields[0]), null)
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
      if (fieldList.contains(fields[0])) {
        collector.addFailure(
          String.format("Field '%s' cannot be used for both mapping and normalizing.", fields[0]), null)
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
      if (validTypeField == null) {
        validTypeField = fields[1];
      } else if (!validTypeField.equals(fields[1])) {
        collector.addFailure(
          String.format("Invalid normalizing field type output column name '%s'.", fields[1]),
          "All normalizing field type output column names must be the same.")
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
      if (validValueField == null) {
        validValueField = fields[2];
      } else if (!validValueField.equals(fields[2])) {
        collector.addFailure(
          String.format("Invalid normalizing field value output column name '%s'.", fields[2]),
          "All normalizing field value output column names must be the same.")
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
      if (outputSchema.getField(fields[1]) == null) {
        collector.addFailure(
          String.format("Normalizing field type output column name '%s' must be present in output schema.",
                        fields[1]), null)
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
      if (outputSchema.getField(fields[2]) == null) {
        collector.addFailure(
          String.format("Normalizing field value output column name '%s' must be present in output schema.",
                        fields[2]), null)
          .withConfigElement(NormalizeConfig.FIELD_NORMALIZING, fieldNormalizing);
      }
    }
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
    // Get failure collector for updated validation API
    FailureCollector collector = getContext().getFailureCollector();
    initializeFieldData();
    if (outputSchema != null) {
      return;
    }
    try {
      outputSchema = Schema.parseJson(config.outputSchema);
    } catch (Exception e) {
      collector.addFailure("Invalid output schema.", "Output schema must be valid JSON.")
        .withConfigProperty(NormalizeConfig.OUTPUT_SCHEMA);
      throw collector.getOrThrowException();
    }
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

  /**
   * Configuration for the Normalize transform.
   */
  public static class NormalizeConfig extends PluginConfig {
    public static final String FIELD_MAPPING = "fieldMapping";
    public static final String FIELD_NORMALIZING = "fieldNormalizing";
    public static final String OUTPUT_SCHEMA = "outputSchema";

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
        collector.addFailure("'Fields to be Mapped' cannot be empty.", null).withConfigProperty(FIELD_MAPPING);
      }
      if (Strings.isNullOrEmpty(fieldNormalizing)) {
        collector.addFailure("'Fields to be Normalized' cannot be empty.", null).withConfigProperty(FIELD_NORMALIZING);
      }
      if (Strings.isNullOrEmpty(outputSchema)) {
        collector.addFailure("Output schema cannot be empty.", null).withConfigProperty(OUTPUT_SCHEMA);
      }
    }
  }
}
