/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.batch.joiner;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Performs lookup and matches dataset records.
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Lookup")
public class Lookup extends BatchAutoJoiner {

  /**
   * Config for Lookup transform
   */
  public static class Config extends PluginConfig {
    private static final String LOOKUP_DATASET = "lookupDataset";
    private static final String INPUT_KEY_FIELD = "inputKeyField";
    private static final String LOOKUP_KEY_FIELD = "lookupKeyField";
    private static final String LOOKUP_VALUE_FIELD = "lookupValueField";
    private static final String OUTPUT_FIELD = "outputField";
    private static final String DEFAULT_VALUE = "defaultValue";
    public static final String OUTPUT_SCHEMA = "schema";

    @Description("Amongst the inputs connected to the lookup transformation, this determines the input that should " +
      "be used as the lookup dataset. This dataset will be loaded into memory and will be broadcast to all " +
      "executors, so it should be the smaller of the inputs.")
    @Name(LOOKUP_DATASET)
    @Macro
    private String lookupDataset;

    @Description("Field in the input schema that should be used as a key in the lookup condition.")
    @Name(INPUT_KEY_FIELD)
    @Macro
    private final String inputKeyField;

    @Description("Field in the lookup source that should be used as a key in the lookup condition.")
    @Name(LOOKUP_KEY_FIELD)
    @Macro
    private final String lookupKeyField;

    @Description("Field in the lookup source that should be returned after the lookup.")
    @Name(LOOKUP_VALUE_FIELD)
    @Macro
    private final String lookupValueField;

    @Description("Name of the field in which to store the result of the lookup. This field will be added to the " +
      "output schema, and will contain the value of the Lookup Value Field.")
    @Name(OUTPUT_FIELD)
    @Macro
    private final String outputField;

    @Description("Default value to use when there is no match in the lookup source. Defaults to null.")
    @Name(DEFAULT_VALUE)
    @Macro
    @Nullable
    private final String defaultValue;

    @Nullable
    @Macro
    @Description(OUTPUT_SCHEMA)
    private String schema;

    public Config(String lookupDataset, String inputKeyField, String lookupKeyField, String lookupValueField,
                  String outputField, @Nullable String defaultValue, @Nullable String schema) {
      this.lookupDataset = lookupDataset;
      this.inputKeyField = inputKeyField;
      this.lookupKeyField = lookupKeyField;
      this.lookupValueField = lookupValueField;
      this.outputField = outputField;
      this.defaultValue = defaultValue;
      this.schema = schema;
    }

    public String getLookupDataset() {
      return lookupDataset;
    }

    public void setLookupDatasets(Set<String> datasets) {
      this.lookupDataset = String.join(",", datasets);
    }

    public String getInputKeyField() {
      return inputKeyField;
    }

    public String getLookupKeyField() {
      return lookupKeyField;
    }

    public String getLookupValueField() {
      return lookupValueField;
    }

    public String getOutputField() {
      return outputField;
    }

    @Nullable
    public String getDefaultValue() {
      return defaultValue;
    }

    @Nullable
    public Schema getOutputSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema: " + e.getMessage(), null)
          .withConfigProperty(OUTPUT_SCHEMA);
      }
      // if there was an error that was added, it will throw an exception, otherwise,
      // this statement will not be executed
      throw collector.getOrThrowException();
    }

    public boolean fieldsContainMacro() {
      return containsMacro(LOOKUP_DATASET) || containsMacro(LOOKUP_KEY_FIELD) || containsMacro(LOOKUP_VALUE_FIELD)
        || containsMacro(INPUT_KEY_FIELD) || containsMacro(OUTPUT_FIELD);
    }

    private void validate(FailureCollector failureCollector) {
      if (!containsMacro(LOOKUP_DATASET) && Strings.isNullOrEmpty(lookupDataset)) {
        failureCollector.addFailure("Missing lookup dataset.", "Lookup dataset must provided.")
          .withConfigProperty(LOOKUP_DATASET);
      }
      if (!containsMacro(INPUT_KEY_FIELD) && Strings.isNullOrEmpty(inputKeyField)) {
        failureCollector.addFailure("Missing input key field.", "Input key field must be provided.")
          .withConfigProperty(INPUT_KEY_FIELD);
      }
      if (!containsMacro(LOOKUP_KEY_FIELD) && Strings.isNullOrEmpty(lookupKeyField)) {
        failureCollector.addFailure("Missing lookup key field.", "Lookup key field must be provided.")
          .withConfigProperty(LOOKUP_KEY_FIELD);
      }
      if (!containsMacro(LOOKUP_VALUE_FIELD) && Strings.isNullOrEmpty(lookupValueField)) {
        failureCollector.addFailure("Missing lookup value field.", "Lookup value field must be provided.")
          .withConfigProperty(LOOKUP_VALUE_FIELD);
      }
      if (!containsMacro(OUTPUT_FIELD) && Strings.isNullOrEmpty(outputField)) {
        failureCollector.addFailure("Missing output field.", "Output field must be provided.")
          .withConfigProperty(OUTPUT_FIELD);
      }
    }
  }

  private final Lookup.Config config;
  private Schema outputSchema = null;

  public Lookup(Config config) {
    this.config = config;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    FailureCollector collector = context.getFailureCollector();
    boolean hasUnknownInputSchema = context.getInputStages().values().stream().anyMatch(Objects::isNull);
    if (hasUnknownInputSchema) {
      // If input schemas are unknown, an output schema must be provided.
      collector.addFailure("Missing output schema", "Output schema must be specified.");
    }
    if (config.fieldsContainMacro()) {
      return null;
    }
    outputSchema = config.getOutputSchema(collector);


    if (outputSchema == null) {
      final Map<String, Schema> inputSchemas = context.getInputStages().values().stream()
        .collect(Collectors.toMap(e -> e.getStageName(), e -> e.getSchema()));
      outputSchema = generateOutputSchema(inputSchemas, collector);
    }
    boolean useOutputSchema = false;
    List<JoinStage> inputs = new ArrayList<>(context.getInputStages().size());
    try {
      Schema inputSchema = null;
      String inputStageName = "";
      String lookupStageName = config.lookupDataset;
      for (String stageName : context.getInputStages().keySet()) {
        if (!stageName.equals(config.lookupDataset)) {
          inputSchema = context.getInputStages().get(stageName).getSchema();
          inputStageName = stageName;
        }
      }
      for (JoinStage joinStage : context.getInputStages().values()) {
        inputs.add(JoinStage.builder(joinStage)
                     .setRequired(!joinStage.getStageName().equals(config.lookupDataset))
                     .setBroadcast(joinStage.getStageName().equals(config.lookupDataset))
                     .build());
        useOutputSchema = useOutputSchema || joinStage.getSchema() == null;
      }

      JoinDefinition.Builder joinBuilder = JoinDefinition.builder();
      joinBuilder.from(inputs);
      List<JoinField> selectedJoinFields = new ArrayList<>();
      for (Schema.Field field : outputSchema.getFields()) {
        if (!field.getName().equals(config.outputField)) {
          selectedJoinFields.add(new JoinField(inputStageName, field.getName(), field.getName()));
        }
      }
      selectedJoinFields.add(new JoinField(lookupStageName, config.lookupValueField, config.outputField));
      joinBuilder.select(selectedJoinFields);
      // input keys
      joinBuilder.on(JoinCondition.onKeys()
                       .addKey(new JoinKey(inputStageName, Collections.singletonList(config.inputKeyField)))
                       .addKey(new JoinKey(lookupStageName, Collections.singletonList(config.lookupKeyField)))
                       .build());
      joinBuilder.setOutputSchema(outputSchema);
      return joinBuilder.build();
    } catch (Exception e) {
      collector.addFailure("Error:", e.getMessage());
    }
    throw collector.getOrThrowException();
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchJoinerContext context) {
    super.onRunFinish(succeeded, context);
  }

  private Schema generateOutputSchema(Map<String, Schema> inputSchemas, FailureCollector collector) {
    final Set<String> schemas = inputSchemas.keySet();
    if (schemas.size() == 0) {
      collector.addFailure("Missing input schema.", "Please connect valid input datasets.");
    }
    Schema lookupSchema = inputSchemas.getOrDefault(config.lookupDataset, null);
    Schema inputSchema = null;
    if (lookupSchema == null) {
      collector.addFailure("Lookup dataset with name not found.", "Please provide valid name for lookup dataset.");
    }
    for (String schemaName : schemas) {
      if (!schemaName.equals(config.lookupDataset)) {
        inputSchema = inputSchemas.getOrDefault(schemaName, null);
      }
    }
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(config.outputField, Schema.nullableOf(lookupSchema.getField(config.lookupValueField)
                                                                       .getSchema())));
    return Schema.recordOf("join.output", fields);
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer multiInputPipelineConfigurer) {
    super.configurePipeline(multiInputPipelineConfigurer);
    final Map<String, Schema> inputSchemas = multiInputPipelineConfigurer
      .getMultiInputStageConfigurer().getInputSchemas();
    FailureCollector collector = multiInputPipelineConfigurer.getMultiInputStageConfigurer().getFailureCollector();
    config.validate(collector);
    if (config.fieldsContainMacro()) {
      return;
    }
    if (inputSchemas.values().size() != 2) {
      if (inputSchemas.values().size() > 2) {
        collector.addFailure("More than two input datasets provided.",
                             "Only two input dataset are allowed.");
      } else {
        collector.addFailure("Not enough datasets provided.",
                             "Two input dataset are required.");
      }
      collector.getOrThrowException();
    }
    if (!inputSchemas.keySet().contains(config.lookupDataset)) {
      collector.addFailure("Missing lookup dataset.",
                           "Lookup dataset name needs to match one of the input datasets.")
        .withConfigProperty(config.lookupDataset);
    }
    if (inputSchemas.get(config.lookupDataset) != null) {
      if (inputSchemas.get(config.lookupDataset).getField(config.lookupKeyField) == null) {
        collector.addFailure("Lookup key field not found.",
                             "Lookup key field needs ot be one of the lookup dataset fields.")
          .withConfigProperty(config.lookupKeyField);
      }
      if (inputSchemas.get(config.lookupDataset).getField(config.lookupValueField) == null) {
        collector.addFailure("Lookup value field not found.",
                             "Lookup value field needs ot be one of the lookup dataset fields.")
          .withConfigProperty(config.lookupValueField);
      }
    }
    String inputSchemaName = inputSchemas.keySet().stream()
      .filter(inputName -> !inputName.equals(config.lookupDataset)).findFirst().get();
    if (inputSchemas.get(inputSchemaName).getField(config.inputKeyField) == null) {
      collector.addFailure("Input key field not found.",
                           "Input key field needs to be one of the input dataset fields.")
        .withConfigProperty(config.inputKeyField);
      collector.getOrThrowException();
    }
    if (!inputSchemas.get(config.lookupDataset).getField(config.lookupKeyField).getSchema()
      .isCompatible(inputSchemas.get(inputSchemaName).getField(config.inputKeyField).getSchema())) {
      collector.addFailure("Input key field type does not match lookup key field type.",
                           "Input key field type needs to match lookup key field type.");
    }
    if (inputSchemas.get(inputSchemaName).getField(config.outputField) != null) {
      collector.addFailure("Field with name matching output field name already exists in input dataset.",
                           "Output field name should not collide with any input dataset field name.");
    }
    if (!config.containsMacro(Config.OUTPUT_SCHEMA)) {
      Schema tempSchema = config.getOutputSchema(collector);
      if (tempSchema == null) {
        tempSchema = generateOutputSchema(inputSchemas, collector);
      }
      multiInputPipelineConfigurer.getMultiInputStageConfigurer().setOutputSchema(tempSchema);
    }
    collector.getOrThrowException();
  }
}
