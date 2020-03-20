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
import io.cdap.plugin.batch.aggregator.function.SelectionFunction;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

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
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);

    // in configurePipeline all the necessary checks have been performed already to set output schema
    if (SchemaValidator.canRecordLineage(context.getOutputSchema(), context.getStageName())) {
      TransformLineageRecorderUtils.generateOneToOnes(
              TransformLineageRecorderUtils.getFields(context.getInputSchema()),
              "dedup",
              "Removed duplicate records based on unique fields.");
    }
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
    FailureCollector collector = stageConfigurer.getFailureCollector();
    validateSchema(outputSchema, uniqueFields, functionInfo, collector);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    uniqueFields = dedupConfig.getUniqueFields();
    filterFunction = dedupConfig.getFilter();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
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
                        Emitter<StructuredRecord> emitter) {
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

  private void validateSchema(Schema inputSchema, List<String> uniqueFields,
                              @Nullable DedupConfig.DedupFunctionInfo function, FailureCollector collector) {
    for (String uniqueField : uniqueFields) {
      Schema.Field field = inputSchema.getField(uniqueField);
      if (field == null) {
        collector.addFailure(String.format("Field '%s' does not exist in the input schema", uniqueField), "")
          .withConfigElement("uniqueFields", uniqueField);
      }
    }

    if (function != null) {
      Schema.Field field = inputSchema.getField(function.getField());
      if (field == null) {
        collector.addFailure(String.format("Invalid filter %s(%s): Field '%s' does not exist in input schema ",
                                           function.getFunction(), function.getField(), function.getField()),
                             null)
          .withConfigProperty("filterOperation");
      }
    }
  }
}
