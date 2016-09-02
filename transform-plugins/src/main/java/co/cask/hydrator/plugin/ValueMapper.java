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
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupTableConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms records using custom mapping provided by the config.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("ValueMapper")
@Description("Maps and converts record values using a mapping dataset")
public class ValueMapper extends Transform<StructuredRecord, StructuredRecord> {

  private final Config config;
  private final Map<Schema, Schema> schemaCache = new HashMap<>();
  private static final Map<String, ValueMapping> mappingValues = new HashMap<>();
  private static Map<String, String> defaultsMapping = new HashMap<>();
  private Map<String, Lookup<String>> lookupTableCache = new HashMap<String, Lookup<String>>();

  //for unit tests, otherwise config is injected by plugin framework.
  public ValueMapper(Config config) {
    this.config = config;
    this.config.parseConfiguration();
  }

  /**
   * Configuration for the ValueMapper transform.
   */
  public static class Config extends PluginConfig {

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

    /**
     * This method is meant to parse input configuration.
     * It is required to use in configurePiperline as well as transform method.
     * Hence this is implemented as a part of Config class to set configuration once and make it available for
     * subsequent methods.
     */
    private void parseConfiguration() {
      if (!defaults.isEmpty()) {
        String[] defaultsList = this.defaults.split(",");
        for (String defaultValue : defaultsList) {
          String[] defaultsArray = defaultValue.split(":");
          if (defaultsArray.length != 2) {
            throw new IllegalArgumentException("Invalid default " + defaultValue + ". Defaults should contain source" +
                                                 " field and its corresponding default value in the format: " +
                                                 "<source-field>:<default-value>[,<source-field>:<default-value>]*" +
                                                 "For example: lang_code:English,country_code:Britain");
          } else {
            defaultsMapping.put(defaultsArray[0], defaultsArray[1]);
          }
        }
      }
      String[] mappingArray = this.mapping.split(",");
      for (String mapping : mappingArray) {
        String[] mappingValueArray = mapping.split(":");
        if (mappingValueArray.length != 3) {
          throw new IllegalArgumentException("Invalid mapping " + mapping + ". Mapping should contain source field, " +
                                               "lookup table name and target field in the format: " +
                                               "<source-field>:<lookup-table-name>:<target-field>" +
                                               "[,<source-field>:<lookup-table-name>:<target-field>]*" +
                                               "For example: lang_code:language_code_lookup:lang_desc," +
                                               "country_code:country_lookup:country_name");
        } else {
          String defaultValue = null;
          if (defaultsMapping.containsKey(mappingValueArray[0])) {
            defaultValue = defaultsMapping.get(mappingValueArray[0]);
          }
          ValueMapping valueMapping = new ValueMapping(mappingValueArray[2], mappingValueArray[1], defaultValue);

          mappingValues.put(mappingValueArray[0], valueMapping);
        }
      }
    }
  }

  /**
   * Creates output schema with the help of input schema and mapping
   */
  private Schema getOutputSchema(Schema inputSchema) throws IllegalArgumentException {
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
          throw new IllegalArgumentException("Input field " + inputFieldName + " must be of type string, but is" +
                                               " of type" + inputField.getSchema().getType().name());
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
    StructuredRecord.Builder builder = StructuredRecord.builder(getOutputSchema(input.getSchema()));
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
    createLookupTableData(context);
  }

  /**
   * @param pipelineConfigurer
   * @throws IllegalArgumentException when source field is other than String type
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema outputSchema = null;
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      outputSchema = getOutputSchema(inputSchema);
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
