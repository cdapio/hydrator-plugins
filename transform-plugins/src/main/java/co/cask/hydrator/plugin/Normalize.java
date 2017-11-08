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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

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
    config.validate();

    try {
      outputSchema = Schema.parseJson(config.outputSchema);
      for (Schema.Field outputField : outputSchema.getFields()) {
        Schema fieldSchema = outputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() :
          fieldSchema.getType();
        Preconditions.checkArgument(fieldType == Schema.Type.STRING,
                                    "All output schema fields must be of type STRING.");
      }
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    List<String> fieldList = new ArrayList<>();
    //Validate mapping fields
    String[] fieldMappingArray = config.fieldMapping.split(",");
    for (String fieldMapping : fieldMappingArray) {
      String[] mappings = fieldMapping.split(":");
      Preconditions.checkArgument(mappings.length == 2, "Mapping field '" + mappings[0] + "' is invalid. Both input" +
        " and output schema fields required.");
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null) {
        Preconditions.checkArgument(inputSchema.getField(mappings[0]) != null, "Mapping field '" + mappings[0]
          + "' not present in input schema.");
      }
      Preconditions.checkArgument(outputSchema.getField(mappings[1]) != null, "Output schema mapping field '" +
        mappings[1] + "' not present in output schema.");
      fieldList.add(mappings[0]);
    }

    //Validate normalizing fields
    String[] fieldNormalizingArray = config.fieldNormalizing.split(",");

    //Type and Value mapping for all normalize fields must be same, otherwise it is invalid.
    //Read type and value from first normalize fields which is used for validation.
    String[] typeValueFields = fieldNormalizingArray[0].split(":");
    Preconditions.checkArgument(typeValueFields.length == 3, "Normalizing field '" + typeValueFields[0] +
      "' is invalid. Field Type and Field Value columns required.");
    String validTypeField = typeValueFields[1];
    String validValueField = typeValueFields[2];

    for (String fieldNormalizing : fieldNormalizingArray) {
      String[] fields = fieldNormalizing.split(":");
      Preconditions.checkArgument(fields.length == 3, "Normalizing field '" + fields[0] + "' is invalid. " +
        " Field Type and Field Value columns required.");
      //Input schema cannot be null, check added for JUnit test case run.
      if (inputSchema != null) {
        Preconditions.checkArgument(inputSchema.getField(fields[0]) != null, "Normalizing field '" + fields[0]
          + "' not present in input schema.");
      }
      Preconditions.checkArgument(!fieldList.contains(fields[0]), "'" + fields[0] + "' cannot be use for " +
        "both mapping as well as normalize fields.");
      Preconditions.checkArgument(validTypeField.equals(fields[1]), "Type mapping is invalid for " +
        "normalize field '" + fields[0] + "'. It must be same for all normalize fields.");
      Preconditions.checkArgument(validValueField.equals(fields[2]), "Value mapping is invalid for " +
        "normalize field '" + fields[0] + "'. It must be same for all normalize fields.");
      Preconditions.checkArgument(outputSchema.getField(fields[1]) != null, "Type mapping '" + fields[1] +
        "' not present in output schema.");
      Preconditions.checkArgument(outputSchema.getField(fields[1]) != null, "Value mapping '" + fields[2] +
        "' not present in output schema.");
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
    initializeFieldData();
    if (outputSchema != null) {
      return;
    }
    try {
      outputSchema = Schema.parseJson(config.outputSchema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
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

    private void validate() {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldMapping), "Fields to mapped cannot be empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldNormalizing), "Fields to normalized cannot be empty.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(outputSchema), "Output schema cannot be empty.");
    }
  }
}
