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
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Batch RowDenormalizer Aggregator Plugin - It is used to de-normalize data based on the key column.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("RowDenormalizer")
@Description("This plugin is used to convert the raw data into de-normalized data based on the key column." +
  "User is able to specify the list of fields that should be considered in denormalized record. " +
  "Also, users have option to put alias for output field name. " +
  "For example - " +
  "‘ADDRESS’ in input is mapped to ‘addr’ in output schema. This denormalized data would be easy to query.")
public class RowDenormalizerAggregator extends RecordAggregator {
  private final RowDenormalizerConfig conf;
  private Map<String, String> outputMappings;
  private List<String> outputFields;
  private Schema outputSchema;

  public RowDenormalizerAggregator(RowDenormalizerConfig conf) {
    super(conf.numPartitions);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    validateInputFields(pipelineConfigurer.getStageConfigurer().getInputSchema());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.parseSchema());
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    outputFields = conf.getOutputFields();
    outputMappings = conf.getOutputFieldMappings();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    String keyField = conf.getKeyField();
    Schema keyFieldSchema = Schema.recordOf("record", Schema.Field.of(keyField, Schema.of(Schema.Type.STRING)));
    StructuredRecord.Builder builder = StructuredRecord.builder(keyFieldSchema);
    builder.set(keyField, record.get(keyField));
    emitter.emit(builder.build());
  }

  @Override
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (!iterator.hasNext()) {
      return;
    }
    if (outputSchema == null) {
      initializeOutputSchema();
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    builder.set(conf.getKeyField(), groupKey.get(groupKey.getSchema().getFields().get(0).getName()));
    while (iterator.hasNext()) {
      StructuredRecord record = iterator.next();
      String fieldName = record.get(conf.getFieldName());

      if (outputFields.contains(fieldName)) {
        builder.set(fieldName, record.get(conf.getFieldValue()));
      } else {
        if ((outputMappings.containsKey(fieldName)) && (!outputMappings.get(fieldName).
          equals(RowDenormalizerConfig.CHECK_ALIAS))) {
          builder.set(outputMappings.get(fieldName), record.get(conf.getFieldValue()));
        }
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Builds the output schema, using output fields provided by user
   */
  private void initializeOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    // Input fields coming from source will be of String type, that's why we are building the output schema as String.
    fields.add(Schema.Field.of(conf.getKeyField(), Schema.of(Schema.Type.STRING)));
    for (String outputField : outputFields) {
      fields.add(Schema.Field.of(outputField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    outputSchema = Schema.recordOf("group.key.schema", fields);
  }

  /**
   * @param inputSchema Validates whether the keyfield, fieldname and fieldvalue entered by user is present in input
   *                    schema or not.
   * @return true
   */
  private void validateInputFields(Schema inputSchema) {
    if (!inputSchema.getFields().toString().contains(conf.getKeyField())) {
      throw new IllegalArgumentException(
        String.format("Cannot denormalize using this field '%s' as it does not exist in input schema %s",
                      conf.getKeyField(), inputSchema));
    } else if (!inputSchema.getFields().toString().contains(conf.getFieldName())) {
      throw new IllegalArgumentException(
        String.format("Cannot find field '%s' in input schema %s", conf.getFieldName(), inputSchema));
    } else if (!inputSchema.getFields().toString().contains(conf.getFieldValue())) {
      throw new IllegalArgumentException(
        String.format("Cannot find field '%s' in input schema %s", conf.getFieldValue(), inputSchema));
    }
  }
}

