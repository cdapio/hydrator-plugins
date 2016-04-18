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
import co.cask.hydrator.plugin.batch.aggregator.function.RecordAggregateFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Path;

/**
 * Deduplicate aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Deduplicate")
@Description("Deduplicates input records so that all output records are distinct.")
public class DedupAggregator extends RecordAggregator {
  private final DedupConfig dedupConfig;
  private List<String> uniqueFields;
  private DedupConfig.DedupFunctionInfo filterFunction;
  private RecordAggregateFunction recordAggregateFunction;

  public DedupAggregator(DedupConfig dedupConfig) {
    super(dedupConfig.numPartitions);
    this.dedupConfig = dedupConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    List<String> uniqueFields = dedupConfig.getUniqueFields();
    DedupConfig.DedupFunctionInfo functionInfo = dedupConfig.getFilter();

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    // otherwise, we have a constant input schema. Get the output schema and propagate the schema
    stageConfigurer.setOutputSchema(getOuputSchema(inputSchema, uniqueFields, functionInfo));
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

    if (filterFunction == null) {
      emitter.emit(iterator.next());
    } else {
      StructuredRecord firstVal = iterator.next();
      recordAggregateFunction = filterFunction.getAggregateFunction(firstVal.getSchema());
      recordAggregateFunction.beginAggregate();
      recordAggregateFunction.update(firstVal);

      while (iterator.hasNext()) {
        recordAggregateFunction.update(iterator.next());
      }

      recordAggregateFunction.finishAggregate();
      Schema outputSchema = getOuputSchema(firstVal.getSchema(), uniqueFields, filterFunction);
      StructuredRecord outputRecord = recordAggregateFunction.getChosenRecord();
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      for (Schema.Field field : outputRecord.getSchema().getFields()) {
        if (filterFunction != null && Objects.equals(field.getName(), filterFunction.getField())) {
          builder.set(filterFunction.getName(), outputRecord.get(field.getName()));
        } else {
          builder.set(field.getName(), outputRecord.get(field.getName()));
        }
      }
      emitter.emit(builder.build());
    }
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    Schema inputSchema;
    try {
      inputSchema = Schema.parseJson(request.inputSchema);
    } catch (Exception e) {
      throw new BadRequestException("Could not parse input schema " + request.inputSchema);
    }
    try {
      return getOuputSchema(inputSchema, request.getUniqueFields(), request.getFilter());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
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
    return Schema.recordOf(inputSchema.getRecordName() + ".distinct", fields);
  }

  private Schema getOuputSchema(Schema inputSchema, List<String> uniqueFields,
                                DedupConfig.DedupFunctionInfo functionInfo) {
    List<AggregatorConfig.FunctionInfo> functionInfos = new ArrayList<>();
    if (functionInfo != null) {
      functionInfos.add(functionInfo);
    }
    validateSchema(inputSchema, uniqueFields, functionInfos);

    List<Schema.Field> outputFields = new ArrayList<>(inputSchema.getFields().size());
    for (Schema.Field field : inputSchema.getFields()) {
      if (functionInfo != null && Objects.equals(field.getName(), functionInfo.getField())) {
        outputFields.add(Schema.Field.of(functionInfo.getName(), field.getSchema()));
      } else {
        outputFields.add(field);
      }
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".unique", outputFields);
  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends DedupConfig {
    private String inputSchema;
  }

  private void validateSchema(Schema inputSchema, List<String> uniqueFields,
                              List<AggregatorConfig.FunctionInfo> functionInfos) {
    for (String uniqueField : uniqueFields) {
      Schema.Field field = inputSchema.getField(uniqueField);
      if (field == null) {
        throw new IllegalArgumentException(String.format("Cannot do an unique on field '%s' because it does " +
                                                           "not exist in inputSchema '%s'", uniqueField, inputSchema));
      }
    }

    for (AggregatorConfig.FunctionInfo functionInfo : functionInfos) {
      if (functionInfo != null) {
        Schema.Field field = inputSchema.getField(functionInfo.getField());
        if (field == null) {
          throw new IllegalArgumentException(String.format(
            "Invalid filter %s(%s): Field '%s' does not exist in input schema '%s'",
            functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
        }
      }
    }
  }
}
