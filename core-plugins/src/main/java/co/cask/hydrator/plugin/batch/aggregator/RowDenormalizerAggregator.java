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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;

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
    validateInputFields(pipelineConfigurer.getStageConfigurer().getInputSchema());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(initializeOutputSchema());
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
      if (record.getSchema().getField(keyField) != null && !record.getSchema().getField(keyField).getSchema()
        .isNullable()) {
        throw new IllegalArgumentException("null value found for non-nullable field " + keyField);
      }
      return;
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

      if (outputFieldName == null && record.getSchema().getField(nameField) != null && !record.getSchema().getField
        (nameField).getSchema().isNullable()) {
        throw new IllegalArgumentException("null value found for non-nullable field " + nameField);
      }
      if (outputFieldValue == null && record.getSchema().getField(valueField) != null && !record.getSchema().getField
        (valueField).getSchema().isNullable()) {
        throw new IllegalArgumentException("null value found for non-nullable field " + valueField);
      }

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
   * @return true
   */
  private void validateInputFields(Schema inputSchema) {

    Schema.Field keyField = inputSchema.getField(conf.getKeyField());
    Schema.Field nameField = inputSchema.getField(conf.getNameField());
    Schema.Field valueField = inputSchema.getField(conf.getValueField());

    if (keyField == null) {
      throw new IllegalArgumentException(
        String.format("Keyfield '%s' does not exist in input schema %s", conf.getKeyField(), inputSchema));
    } else if (nameField == null) {
      throw new IllegalArgumentException(
        String.format("Namefield '%s' does not exist in input schema %s", conf.getNameField(), inputSchema));
    } else if (valueField == null) {
      throw new IllegalArgumentException(
        String.format("Valuefield '%s' does not exist in input schema %s", conf.getValueField(), inputSchema));
    } else if (!((keyField.getSchema().isNullable() ? keyField.getSchema().getNonNullable().getType() : keyField
      .getSchema().getType()).equals(Schema.Type.STRING))) {
      throw new IllegalArgumentException(
        String.format("Keyfield '%s' in the input record must be of type String or Nullable String",
                      conf.getKeyField()));
    } else if (!((nameField.getSchema().isNullable() ? nameField.getSchema().getNonNullable().getType() : nameField
      .getSchema().getType()).equals(Schema.Type.STRING))) {
      throw new IllegalArgumentException(
        String.format("Namefield '%s' in the input record must be of type String or Nullable String",
                      conf.getNameField()));
    } else if (!((valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable().getType() : valueField
      .getSchema().getType()).equals(Schema.Type.STRING))) {
      throw new IllegalArgumentException(
        String.format("Valuefield '%s' in the input record must be of type String or Nullable String",
                      conf.getValueField()));
    }
  }
}
