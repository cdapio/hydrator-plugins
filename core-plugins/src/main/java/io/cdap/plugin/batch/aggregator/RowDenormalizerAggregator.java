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

package io.cdap.plugin.batch.aggregator;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch RowDenormalizer Aggregator Plugin - It is used to de-normalize data based on the key column.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("RowDenormalizer")
@Description("This plugin is used to convert raw data into denormalized data based on a key column. " +
  "User is able to specify the list of fields that should be used in the denormalized record, with " +
  "an option to use an alias for the output field name. " +
  "For example, " +
  "'ADDRESS' in the input is mapped to 'addr' in the output schema. The denormalized data is easier to query.")
public class RowDenormalizerAggregator extends BatchAggregator<String, StructuredRecord, StructuredRecord> {

  private static final String KEY_FIELD = "keyField";
  private static final String NAME_FIELD = "nameField";
  private static final String VALUE_FIELD = "valueField";
  private final RowDenormalizerConfig conf;
  private Map<String, String> outputMappings;
  private Set<String> outputFields;
  private Schema outputSchema;
  private String keyField;
  private String nameField;
  private String valueField;

  public RowDenormalizerAggregator(RowDenormalizerConfig conf) {
    this.conf = conf;
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    if (conf.numPartitions != null) {
      context.setNumPartitions(conf.numPartitions);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (validateInputFields(inputSchema, stageConfigurer.getFailureCollector())) {
      stageConfigurer.setOutputSchema(initializeOutputSchema());
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    outputFields = conf.getOutputSchemaFields();
    outputMappings = conf.getFieldAliases();
    keyField = conf.getKeyField();
    nameField = conf.getNameField();
    valueField = conf.getValueField();
    outputSchema = initializeOutputSchema();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<String> emitter) throws Exception {
    if (record.get(keyField) == null) {
      if (record.getSchema().getField(keyField) == null) {
        throw new IllegalArgumentException(
          String.format("Keyfield '%s' does not exist in input schema %s", keyField, record.getSchema()));
      }
      return;
    }
    if (record.get(nameField) == null && record.getSchema().getField(nameField) == null) {
      throw new IllegalArgumentException(
        String.format("Namefield '%s' does not exist in input schema %s", nameField, record.getSchema()));
    }
    if (record.get(valueField) == null && record.getSchema().getField(valueField) == null) {
      throw new IllegalArgumentException(
        String.format("Valuefield '%s' does not exist in input schema %s", valueField, record.getSchema()));
    }
    emitter.emit((String) record.get(keyField));
  }

  @Override
  public void aggregate(String groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (!iterator.hasNext()) {
      return;
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    builder.set(conf.getKeyField(), groupKey);
    while (iterator.hasNext()) {
      StructuredRecord record = iterator.next();
      String outputFieldName = record.get(nameField);
      String outputFieldValue = record.get(valueField);

      outputFieldName = outputMappings.containsKey(outputFieldName) ? outputMappings.get(outputFieldName) :
        outputFieldName;
      if (outputFields.contains(outputFieldName)) {
        builder.set(outputFieldName, outputFieldValue);
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Builds the output schema, using output fields provided by user.
   */
  private Schema initializeOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    // Input fields coming from source will be of type String; that's why we are building the output schema as String.
    fields.add(Schema.Field.of(conf.getKeyField(), Schema.of(Schema.Type.STRING)));
    for (String outputField : conf.getOutputSchemaFields()) {
      fields.add(Schema.Field.of(outputField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("output.schema", fields);
  }

  /**
   * @param inputSchema Validates whether the keyfield, fieldname, and fieldvalue entered by the user is of type
   *                    String or Nullable String and present in the input schema or not.
   * @param collector FailureCollector used to log all errors and return them to the user.
   * @returns true if all fields have been validated and output schema can be set.
   */
  private boolean validateInputFields(Schema inputSchema, FailureCollector collector) {

    boolean canSetOutputSchema = true;
    if (inputSchema == null) {
      return false;
    }

    if (conf.containsMacro(RowDenormalizerConfig.KEY_FIELD)) {
      canSetOutputSchema = false;
    } else {
      if (inputSchema.getField(conf.getKeyField()) == null) {
        collector.addFailure(String.format("Key field '%s' does not exist in input schema", conf.getKeyField()), null)
          .withConfigProperty(KEY_FIELD);
      } else {
        Schema keyFieldSchema = inputSchema.getField(conf.getKeyField()).getSchema();

        final Schema.Type schemaType = keyFieldSchema.isNullable() ?
          keyFieldSchema.getNonNullable().getType() :
          keyFieldSchema.getType();

        if (!schemaType.equals(Schema.Type.STRING)) {
          collector.addFailure(String.format("Key field '%s' in the input record must be a String", conf.getKeyField()),
                               null)
            .withConfigProperty(KEY_FIELD);
        }
      }
    }

    if (conf.containsMacro(RowDenormalizerConfig.NAME_FIELD)) {
      canSetOutputSchema = false;
    } else {
      if (inputSchema.getField(conf.getNameField()) == null) {
        collector.addFailure(String.format("Name field '%s' does not exist in input schema", conf.getNameField()),
                             null)
          .withConfigProperty(NAME_FIELD);
      } else {
        Schema nameFieldSchema = inputSchema.getField(conf.getNameField()).getSchema();

        if (!((nameFieldSchema.isNullable() ? nameFieldSchema.getNonNullable().getType() :
          nameFieldSchema.getType()).equals(Schema.Type.STRING))) {
          collector.addFailure(String.format("Name field '%s' in the input record must be a String",
                               conf.getNameField()), null)
            .withConfigProperty(NAME_FIELD);
        }
      }
    }

    if (conf.containsMacro(RowDenormalizerConfig.VALUE_FIELD)) {
      canSetOutputSchema = false;
    } else {
      if (inputSchema.getField(conf.getValueField()) == null) {
        collector.addFailure(String.format("Value field '%s' does not exist in input schema", conf.getValueField()),
                             null)
          .withConfigProperty(VALUE_FIELD);
      } else {
        Schema valueFieldSchema = inputSchema.getField(conf.getValueField()).getSchema();

        if (!((valueFieldSchema.isNullable() ? valueFieldSchema.getNonNullable().getType() : valueFieldSchema
          .getType()).equals(Schema.Type.STRING))) {
          collector.addFailure(String.format("Value field '%s' in the input record must a String",
                               conf.getValueField()), null)
            .withConfigProperty(VALUE_FIELD);
        }
      }
    }

    if (conf.containsMacro(RowDenormalizerConfig.OUTPUT_FIELDS)) {
      canSetOutputSchema = false;
    }

    return canSetOutputSchema;
  }
}
