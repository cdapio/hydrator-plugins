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
import co.cask.cdap.api.dataset.DatasetProperties;
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
            "For eg: lang_code:language_code_lookup:lang_desc,country_code:country_lookup:country_name")
    private final String mapping;

    @Name("defaults")
    @Description("Specify the defaults for source fields if the lookup does not exist or inputs are NULL or EMPTY. " +
            "Format is <source-field>:<default-value>[,<source-field>:<default-value>]*" +
            "lang_code:English,country_code:Britain")
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
      Map<String, String> defaultsMapping = new HashMap<>();
      if (!defaults.isEmpty()) {
        String[] defaultsList = this.defaults.split(",");
        for (String defaultValue : defaultsList) {
          String[] defaultsArray = defaultValue.split(":");
          defaultsMapping.put(defaultsArray[0], defaultsArray[1]);
        }
      }
      String[] mappingArray = this.mapping.split(",");
      for (String mapping : mappingArray) {
        String[] mappingValueArray = mapping.split(":");
        ValueMapping valueMapping = new ValueMapping();
        valueMapping.setTargetField(mappingValueArray[2]);
        valueMapping.setLookupTableName(mappingValueArray[1]);

        if (defaultsMapping.containsKey(mappingValueArray[0])) {
          valueMapping.setDefaultValue(defaultsMapping.get(mappingValueArray[0]));
        }
        mappingValues.put(mappingValueArray[0], valueMapping);
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
      if (mappingValues.containsKey(inputField.getName())) {
        if (inputField.getSchema().getType() != Schema.Type.STRING && !inputField.getSchema().isNullable()) {
          throw new IllegalArgumentException("Input field " + inputField.getName() + " type should be String");
        } else {
          outputFields.add(Schema.Field.of(mappingValues.get(inputField.getName()).getTargetField(),
                                           inputField.getSchema()));
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
  private void setLookup(TransformContext context) {
    for (String key :mappingValues.keySet()) {
      ValueMapping mapping = mappingValues.get(key);
      LookupTableConfig tableConfig = new LookupTableConfig(LookupTableConfig.TableType.DATASET);
      DatasetProperties arguments = DatasetProperties.builder().addAll(tableConfig.getDatasetProperties()).build();
      Lookup<String> lookupTable = context.provide(mapping.getLookupTableName(), arguments.getProperties());
      mapping.setLookupTable(lookupTable);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {

    StructuredRecord.Builder builder = StructuredRecord.builder(getOutputSchema(input.getSchema()));

    for (Schema.Field sourceField : input.getSchema().getFields()) {
      if (mappingValues.containsKey(sourceField.getName())) {
        ValueMapping mapping = mappingValues.get(sourceField.getName());
        if (input.get(sourceField.getName()) == null || input.get(sourceField.getName()).toString().isEmpty()) {
          if (mapping.getDefaultValue() != null) {
            builder.set(mapping.getTargetField(), mapping.getDefaultValue());
          } else {
            builder.set(mapping.getTargetField(), input.get(sourceField.getName()));
          }
        } else {
          // for those source field whose values are neither NULL nor EMPTY
          Lookup<String> valueMapperLookUp = mapping.getLookupTable();
          String lookupValue = valueMapperLookUp.lookup(input.get(sourceField.getName()).toString());
          if (lookupValue != null && !lookupValue.isEmpty()) {
            builder.set(mapping.getTargetField(), lookupValue);
          } else {
            builder.set(mapping.getTargetField(), mapping.getDefaultValue());
          }
        }
      } else {
        // for the fields except source fields
        builder.set(sourceField.getName(), input.get(sourceField.getName()));
      }
    }

    emitter.emit(builder.build());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    setLookup(context);
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

    private Lookup<String> lookupTable;
    private String targetField;
    private String lookupTableName;
    private String defaultValue;

    public ValueMapping(){
    }

    public Lookup<String> getLookupTable() {
      return lookupTable;
    }

    public void setLookupTable(Lookup<String> lookupTable) {
      this.lookupTable = lookupTable;
    }

    public String getTargetField() {
      return targetField;
    }

    public void setTargetField(String targetField) {
      this.targetField = targetField;
    }

    public String getLookupTableName() {
      return lookupTableName;
    }

    public void setLookupTableName(String lookupTableName) {
      this.lookupTableName = lookupTableName;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
    }
  }

}
