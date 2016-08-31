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
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.hydrator.plugin.batch.aggregator.function.SelectionFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.Path;

/**
 * Deduplicate aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Deduplicate")
@Description("Deduplicates input records, optionally restricted to one or more fields. Takes an optional " +
  "filter function to choose one or more records based on a specific field and a selection function.")
public class DedupAggregator extends RecordAggregator {
  private final DedupConfig dedupConfig;
  private List<String> uniqueFields;
  private DedupConfig.DedupFunctionInfo filterFunction;

  public DedupAggregator(DedupConfig dedupConfig) {
    super(dedupConfig.numPartitions);
    this.dedupConfig = dedupConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    List<String> uniqueFields = dedupConfig.getUniqueFields();
    DedupConfig.DedupFunctionInfo functionInfo = dedupConfig.getFilter();
    if (functionInfo != null) {
      // Invoke to validate whether the function used is supported
      functionInfo.getSelectionFunction(null);
    }

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    // otherwise, we have a constant input schema. Get the output schema and propagate the schema
    Schema outputSchema = getOutputSchema(inputSchema);
    validateSchema(outputSchema, uniqueFields, functionInfo);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    uniqueFields = dedupConfig.getUniqueFields();
    filterFunction = dedupConfig.getFilter();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    if (uniqueFields == null) {
      emitter.emit(record);
      return;
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
    for (String fieldName : uniqueFields) {
      builder.set(fieldName, record.get(fieldName));
    }
    emitter.emit(builder.build());
  }

  @Override
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) throws Exception {
    if (!iterator.hasNext()) {
      return;
    }

    SelectionFunction selectionFunction;
    if (filterFunction == null) {
      emitter.emit(iterator.next());
    } else {
      StructuredRecord firstRecord = iterator.next();
      Schema.Field firstField = firstRecord.getSchema().getField(filterFunction.getField());
      selectionFunction = filterFunction.getSelectionFunction(firstField.getSchema());
      selectionFunction.beginFunction();
      selectionFunction.operateOn(firstRecord);

      while (iterator.hasNext()) {
        selectionFunction.operateOn(iterator.next());
      }

      List<StructuredRecord> outputRecords = selectionFunction.getSelectedRecords();
      for (StructuredRecord outputRecord : outputRecords) {
        Schema outputSchema = getOutputSchema(outputRecord.getSchema());
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputRecord.getSchema().getFields()) {
          builder.set(field.getName(), outputRecord.get(field.getName()));
        }
        emitter.emit(builder.build());
      }
    }
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema);
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (String fieldName : dedupConfig.getUniqueFields()) {
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        throw new IllegalArgumentException(String.format("Field %s does not exist in input schema %s.",
                                                         fieldName, inputSchema));
      }
      fields.add(field);
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".unique", fields);
  }

  private Schema getOutputSchema(Schema inputSchema) {
    return Schema.recordOf(inputSchema.getRecordName() + ".dedup", inputSchema.getFields());
  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends DedupConfig {
    private Schema inputSchema;
  }

  private void validateSchema(Schema inputSchema, List<String> uniqueFields, DedupConfig.DedupFunctionInfo function) {
    for (String uniqueField : uniqueFields) {
      Schema.Field field = inputSchema.getField(uniqueField);
      if (field == null) {
        throw new IllegalArgumentException(String.format("Cannot do an unique on field '%s' because it does " +
                                                           "not exist in inputSchema '%s'", uniqueField, inputSchema));
      }
    }

    Schema.Field field = inputSchema.getField(function.getField());
    if (field == null) {
      throw new IllegalArgumentException(String.format(
        "Invalid filter %s(%s): Field '%s' does not exist in input schema '%s'",
        function.getFunction(), function.getField(), function.getField(), inputSchema));
    }
  }
}
