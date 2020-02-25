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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.LookupTableConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transforms records using custom mapping provided by the config.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("ValueMapper")
@Requirements(datasetTypes = Table.TYPE)
@Description("Maps and converts record values using a mapping dataset")
public class ValueMapper extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private final Map<Schema, Schema> schemaCache = new HashMap<>();
  private final Map<String, ValueMapping> mappingValues = new HashMap<>();
  private Map<String, String> defaultsMapping = new HashMap<>();
  private Map<String, Lookup<String>> lookupTableCache = new HashMap<String, Lookup<String>>();

  //for unit tests, otherwise config is injected by plugin framework.
  public ValueMapper(Config config) {
    this.config = config;
  }

  /**
   * Configuration for the ValueMapper transform.
   */
  public static class Config extends PluginConfig {
    public static final String DEFAULTS = "defaults";
    public static final String MAPPING = "mapping";

    @Name("mapping")
    @Description("Specify the source and target field mapping and lookup dataset name." +
            "Format is <source-field>:<lookup-table-name>:<target-field>" +
            "[,<source-field>:<lookup-table-name>:<target-field>]*" +
            "Source and target field can only of type string." +
            "For example: lang_code:language_code_lookup:lang_desc,country_code:country_lookup:country_name")
    private final String mapping;

    @Name("defaults")
    @Description("Specify the defaults for source fields if the lookup does not exist or inputs are NULL or EMPTY. " +
            "Format is <source-field>:<default-value>[,<source-field>:<default-value>]*" +
            "For example: lang_code:English,country_code:Britain")
    private final String defaults;

    public Config(String mapping, String defaults) {
      this.mapping = mapping;
      this.defaults = defaults;
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    if (context.getInputSchema() == null || context.getInputSchema().getFields() == null) {
      return;
    }
    parseConfiguration(this.config, context.getFailureCollector());

    // After extracting the mappings, store a list of operations containing identity transforms for every output
    // field also present in the mappings list.
    List<String> mappedFields = TransformLineageRecorderUtils.getFields(context.getInputSchema())
      .stream().filter(mappingValues::containsKey).collect(Collectors.toList());

    List<String> identityFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    identityFields.removeAll(mappedFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(mappedFields.stream().map(sourceFieldName -> new FieldTransformOperation(
      "mapValueOf" + sourceFieldName, "Mapped values of fields based on the lookup table.",
      Collections.singletonList(sourceFieldName), mappingValues.get(sourceFieldName).getDefaultValue())).collect(
        Collectors.toList()));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  /**
   * This method is meant to parse input configuration.
   * It is required to use in configurePiperline as well as transform method (run in initialize).
   */
  private void parseConfiguration(Config config, FailureCollector collector) {
    if (!config.defaults.isEmpty()) {
      String[] defaultsList = config.defaults.split(",");
      for (String defaultValue : defaultsList) {
        String[] defaultsArray = defaultValue.split(":");
        if (defaultsArray.length != 2) {
          collector.addFailure(
            String.format("Invalid default: %s.", defaultValue),
            "Defaults should contain source field and its corresponding default " +
              "value in the format: <source-field>:<default-value>[,<source-field>:<default-value>]* " +
              "For example: lang_code:English,country_code:Britain").withConfigElement(Config.DEFAULTS, defaultValue);
        } else {
          defaultsMapping.put(defaultsArray[0], defaultsArray[1]);
        }
      }
    }
    String[] mappingArray = config.mapping.split(",");
    for (String mapping : mappingArray) {
      String[] mappingValueArray = mapping.split(":");
      if (mappingValueArray.length != 3) {
        collector.addFailure(String.format("Invalid mapping: %s.", mapping),
                             "Mapping should contain source field, lookup table name, " +
                               "and target field in the format: <source-field>:<lookup-table-name>:<target-field>" +
                               "[,<source-field>:<lookup-table-name>:<target-field>]* " +
                               "For example: lang_code:language_code_lookup:lang_desc," +
                               "country_code:country_lookup:country_name").withConfigElement(Config.MAPPING, mapping);
      } else {
        String defaultValue = null;
        if (defaultsMapping.containsKey(mappingValueArray[0])) {
          defaultValue = defaultsMapping.get(mappingValueArray[0]);
        }
        ValueMapping valueMapping = new ValueMapping(mappingValueArray[2], mappingValueArray[1], defaultValue);
        mappingValues.put(mappingValueArray[0], valueMapping);
      }
    }
    collector.getOrThrowException();
  }

  /**
   * Creates output schema with the help of input schema and mapping
   */
  private Schema getOutputSchema(Schema inputSchema, FailureCollector collector) throws IllegalArgumentException {
    Schema outputSchema = schemaCache.get(inputSchema);
    if (outputSchema != null) {
      return outputSchema;
    }

    List<Schema.Field> outputFields = new ArrayList<>();
    for (Schema.Field inputField : inputSchema.getFields()) {
      String inputFieldName = inputField.getName();
      if (mappingValues.containsKey(inputFieldName)) {
        Schema fieldSchema = inputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema
          .getType();
        if (fieldType != Schema.Type.STRING) {
          ValueMapping mapping = mappingValues.get(inputFieldName);
          collector.addFailure(
            String.format("Invalid input field type '%s' for mapped field '%s'.",
                          inputField.getSchema().getDisplayName(), inputFieldName),
              "Only input fields of type string can be mapped.")
            .withInputSchemaField(inputFieldName)
            .withConfigElement(Config.MAPPING, String.format("%s:%s:%s", inputFieldName,
                                                             mapping.getLookupTableName(), mapping.getTargetField()));
        } else {
          //Checks whether user has provided default value for source field
          if (defaultsMapping.containsKey(inputFieldName)) {
            outputFields.add(Schema.Field.of(mappingValues.get(inputFieldName).getTargetField(),
                                             Schema.of(Schema.Type.STRING)));
          } else {
            outputFields.add(Schema.Field.of(mappingValues.get(inputFieldName).getTargetField(),
                                             Schema.nullableOf(Schema.of(Schema.Type.STRING))));
          }
        }
      } else {
        outputFields.add(inputField);
      }
    }
    collector.getOrThrowException();
    outputSchema = Schema.recordOf(inputSchema.getRecordName() + ".formatted", outputFields);
    schemaCache.put(inputSchema, outputSchema);
    return outputSchema;
  }

  /**
   * retrieve lookup table from table name
   */
  private void createLookupTableData(TransformContext context) {
    for (String key : mappingValues.keySet()) {
      ValueMapping mapping = mappingValues.get(key);
      String lookupTableName = mapping.getLookupTableName();
      if (!lookupTableCache.containsKey(lookupTableName)) {
        LookupTableConfig tableConfig = new LookupTableConfig(LookupTableConfig.TableType.DATASET);
        Lookup<String> lookupTable = context.provide(lookupTableName, tableConfig.getDatasetProperties());
        lookupTableCache.put(lookupTableName, lookupTable);
      }
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(
        getOutputSchema(input.getSchema(), getContext().getFailureCollector()));
    for (Schema.Field sourceField : input.getSchema().getFields()) {
      String sourceFieldName = sourceField.getName();
      if (mappingValues.containsKey(sourceFieldName)) {
        ValueMapping mapping = mappingValues.get(sourceFieldName);
        String sourceVal = input.get(sourceFieldName);
        if (sourceVal == null || sourceVal.isEmpty()) {
          if (mapping.getDefaultValue() != null) {
            builder.set(mapping.getTargetField(), mapping.getDefaultValue());
          } else {
            builder.set(mapping.getTargetField(), sourceVal);
          }
        } else {
          // for those source field whose values are neither NULL nor EMPTY
          Lookup<String> valueMapperLookUp = lookupTableCache.get(mapping.getLookupTableName());
          String lookupValue = valueMapperLookUp.lookup(sourceVal);
          if (lookupValue != null && !lookupValue.isEmpty()) {
            builder.set(mapping.getTargetField(), lookupValue);
          } else {
            builder.set(mapping.getTargetField(), mapping.getDefaultValue());
          }
        }
      } else {
        // for the fields except source fields
        builder.set(sourceFieldName, input.get(sourceFieldName));
      }
    }

    emitter.emit(builder.build());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(this.config, context.getFailureCollector());
    createLookupTableData(context);
  }

  /**
   * @param pipelineConfigurer
   * @throws IllegalArgumentException when source field is other than String type
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    parseConfiguration(this.config, collector);
    super.configurePipeline(pipelineConfigurer);
    Schema outputSchema = null;
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      // validate that the mappings don't use input fields that don't exist
      for (Map.Entry<String, ValueMapping> mappingPair: mappingValues.entrySet()) {
        if (inputSchema.getField(mappingPair.getKey()) == null) {
          collector.addFailure(
            String.format("Map key '%s' must be present in the input schema.", mappingPair.getKey()), null)
          .withConfigElement(
            Config.MAPPING,
            String.format("%s:%s:%s", mappingPair.getKey(), mappingPair.getValue().getLookupTableName(),
                          mappingPair.getValue().getTargetField()));
        }
      }
      // validate that the defaults don't use source fields that aren't mapped
      for (Map.Entry<String, String> defaultPair: defaultsMapping.entrySet()) {
        if (!mappingValues.containsKey(defaultPair.getKey())) {
          collector.addFailure(
            String.format("Defaults key '%s' must be present as a source in mapping.", defaultPair.getKey()), null)
            .withConfigElement(Config.DEFAULTS, String.format("%s:%s", defaultPair.getKey(), defaultPair.getValue()));
        }
      }
      outputSchema = getOutputSchema(inputSchema, collector);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  /**
   * Object used to keep input mapping corresponding to each source field
   */
  public static class ValueMapping {

    private String targetField;
    private String lookupTableName;
    private String defaultValue;

    public ValueMapping(String targetField, String lookupTableName, String defaultValue) {
      this.targetField = targetField;
      this.lookupTableName = lookupTableName;
      this.defaultValue = defaultValue;
    }

    public String getTargetField() {
      return targetField;
    }

    public String getLookupTableName() {
      return lookupTableName;
    }

    public String getDefaultValue() {
      return defaultValue;
    }
  }

}
