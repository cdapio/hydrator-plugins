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
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.batch.aggregator.function.AggregateFunction;
import io.cdap.plugin.common.SchemaValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Batch group by aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("GroupByAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group. " +
  "Supports avg, count, count(*), first, last, max, min, and sum as aggregate functions.")
public class GroupByAggregator extends RecordReducibleAggregator<AggregateResult> {
  private final GroupByConfig conf;
  private final HashMap<String, String> functionNameMap = new HashMap<String, String>() {{
    put("AVG", "Avg");
    put("COUNT", "Count");
    put("FIRST", "First");
    put("LAST", "Last");
    put("MAX", "Max");
    put("MIN", "Min");
    put("STDDEV", "Stddev");
    put("SUM", "Sum");
    put("VARIANCE", "Variance");
    put("COLLECTLIST", "CollectList");
    put("COLLECTSET", "CollectSet");
    put("LATESTDATE", "LatestDate");
    put("EARLIESTDATE", "EarliestDate");
    put("LONGESTSTRING", "LongestString");
    put("SHORTESTSTRING", "ShortestString");
    put("COUNTNULLS", "CountNulls");
    put("CONCAT", "Concat");
    put("CONCATDISTINCT", "ConcatDistinct");
    put("LOGICALAND", "LogicalAnd");
    put("LOGICALOR", "LogicalOr");
    put("CORRECTEDSUMOFSQUARES", "CorrectedSumOfSquares");
    put("SUMOFSQUARES", "SumOfSquares");
  }};

  private List<String> groupByFields;
  private List<GroupByConfig.FunctionInfo> functionInfos;
  private Schema outputSchema;

  public GroupByAggregator(GroupByConfig conf) {
    super(conf.numPartitions);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    List<String> groupByFields = conf.getGroupByFields();
    List<GroupByConfig.FunctionInfo> aggregates = conf.getAggregates();

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    // if groupByFields is empty or aggregates is empty, that means they contain macros, which means the
    // output schema is not known at configure time.
    if (inputSchema == null || groupByFields.isEmpty() || aggregates.isEmpty()) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    validate(inputSchema, groupByFields, aggregates, stageConfigurer.getFailureCollector());
    //Throw here to avoid throwing IllegalArgumentExceptions in the next function call
    stageConfigurer.getFailureCollector().getOrThrowException();

    // otherwise, we have a constant input schema. Get the output schema and
    // propagate the schema, which is group by fields + aggregate fields
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, groupByFields, aggregates));
  }

  public void validate(Schema inputSchema, List<String> groupByFields,
                       List<GroupByConfig.FunctionInfo> aggregates, FailureCollector collector) {

    for (String groupByField : groupByFields) {
      Schema.Field field = inputSchema.getField(groupByField);
      if (field == null) {
        collector.addFailure(String.format("Cannot group by field '%s' because it does not exist in input schema.",
                                           groupByField), null)
          .withConfigElement("groupByFields", groupByField);
      }
    }

    for (GroupByConfig.FunctionInfo functionInfo : aggregates) {
      if (functionInfo.getField().equals("*")) {
        continue;
      }
      Schema.Field inputField = inputSchema.getField(functionInfo.getField());
      String collectorFieldName = String.format("%s:%s(%s)", functionInfo.getName(),
                                              functionNameMap.get(functionInfo.getFunction().toString().toUpperCase()),
                                              functionInfo.getField());

      if (inputField == null) {
        collector.addFailure(
          String.format("Invalid aggregate %s(%s): Field '%s' does not exist in input schema.",
                        functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField()), null)
          .withConfigElement("aggregates", collectorFieldName);
      }
    }


  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    super.prepareRun(context);
    LinkedList<FieldOperation> fllOperations = new LinkedList<>();
    // in configurePipeline all the necessary checks have been performed already to set output schema
    if (SchemaValidator.canRecordLineage(context.getOutputSchema(), "output")) {
      Schema inputSchema = context.getInputSchema();
      // for every function record the field level operation details
      for (GroupByConfig.FunctionInfo functionInfo : conf.getAggregates()) {
        Schema.Field outputSchemaField = getOutputSchemaField(functionInfo, inputSchema);
        String operationName = String.format("Group %s", functionInfo.getField());
        String description = String.format("Aggregate function applied: '%s'.", functionInfo.getFunction());
        FieldOperation operation = new FieldTransformOperation(operationName, description,
                                                               Collections.singletonList(functionInfo.getField()),
                                                               outputSchemaField.getName());
        fllOperations.add(operation);
      }
    }
    context.record(fllOperations);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    groupByFields = conf.getGroupByFields();
    functionInfos = conf.getAggregates();
    if (context.getInputSchema() != null) {
      initAggregates(context.getInputSchema());
    }
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    // app should provide some way to make some data calculated in configurePipeline available here.
    // then we wouldn't have to calculate schema here
    StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
    for (String groupByField : conf.getGroupByFields()) {
      builder.set(groupByField, record.get(groupByField));
    }
    emitter.emit(builder.build());
  }

  @Override
  public AggregateResult initializeAggregateValue(StructuredRecord record) {
    Map<String, AggregateFunction> functions = initAggregates(record.getSchema());
    functions.values().forEach(AggregateFunction::initialize);
    updateAggregates(functions, record);
    return new AggregateResult(record.getSchema(), functions);
  }

  @Override
  public AggregateResult mergeValues(AggregateResult agg, StructuredRecord record) {
    updateAggregates(agg.getFunctions(), record);
    return agg;
  }

  @Override
  public AggregateResult mergePartitions(AggregateResult agg1, AggregateResult agg2) {
    mergeAggregates(agg1.getFunctions(), agg2.getFunctions());
    return agg1;
  }

  @Override
  public void finalize(StructuredRecord groupKey, AggregateResult aggValue,
                       Emitter<StructuredRecord> emitter) {
    initAggregates(aggValue.getInputSchema());
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (String groupByField : groupByFields) {
      builder.set(groupByField, groupKey.get(groupByField));
    }

    for (Map.Entry<String, AggregateFunction> aggregateFunction : aggValue.getFunctions().entrySet()) {
      builder.set(aggregateFunction.getKey(), aggregateFunction.getValue().getAggregate());
    }
    emitter.emit(builder.build());
  }

  private Schema getOutputSchema(Schema inputSchema, List<String> groupByFields,
                                 List<GroupByConfig.FunctionInfo> aggregates) {
    // Check that all the group by fields exist in the input schema,
    List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
    for (String groupByField : groupByFields) {
      Schema.Field field = inputSchema.getField(groupByField);
      if (field == null) {
        throw new IllegalArgumentException(String.format(
          "Cannot group by field '%s' because it does not exist in input schema %s.",
          groupByField, inputSchema));
      }
      outputFields.add(field);
    }

    // add all the required output field schema depending on the aggregate functions
    for (GroupByConfig.FunctionInfo functionInfo : aggregates) {
      outputFields.add(getOutputSchemaField(functionInfo, inputSchema));
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".agg", outputFields);
  }

  private void updateAggregates(Map<String, AggregateFunction> aggregateFunctions, StructuredRecord groupVal) {
    for (AggregateFunction aggregateFunction : aggregateFunctions.values()) {
      aggregateFunction.mergeValue(groupVal);
    }
  }

  private void mergeAggregates(Map<String, AggregateFunction> agg1, Map<String, AggregateFunction> agg2) {
    for (Map.Entry<String, AggregateFunction> aggregateFunction : agg1.entrySet()) {
      aggregateFunction.getValue().mergeAggregates(agg2.get(aggregateFunction.getKey()));
    }
  }

  private Schema.Field getOutputSchemaField(GroupByConfig.FunctionInfo functionInfo, Schema inputSchema) {
    // special case count(*) because we don't have to check that the input field exists
    if (functionInfo.getField().equals("*")) {
      AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(null);
      return Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema());
    }

    Schema.Field inputField = inputSchema.getField(functionInfo.getField());
    if (inputField == null) {
      throw new IllegalArgumentException(String.format(
        "Invalid aggregate %s(%s): Field '%s' does not exist in input schema %s.",
        functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
    }
    AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputField.getSchema());
    return Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema());
  }

  private Map<String, AggregateFunction> initAggregates(Schema valueSchema) {
    List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + functionInfos.size());
    for (String groupByField : groupByFields) {
      outputFields.add(valueSchema.getField(groupByField));
    }

    Map<String, AggregateFunction> functions = new HashMap<>();
    for (GroupByConfig.FunctionInfo functionInfo : functionInfos) {
      Schema.Field inputField = valueSchema.getField(functionInfo.getField());
      Schema fieldSchema = inputField == null ? null : inputField.getSchema();
      AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(fieldSchema);
      outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
      functions.put(functionInfo.getName(), aggregateFunction);
    }
    outputSchema = Schema.recordOf(valueSchema.getRecordName() + ".agg", outputFields);
    return functions;
  }

  private Schema getGroupKeySchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    for (String groupByField : conf.getGroupByFields()) {
      Schema.Field fieldSchema = inputSchema.getField(groupByField);
      if (fieldSchema == null) {
        throw new IllegalArgumentException(String.format(
          "Cannot group by field '%s' because it does not exist in input schema %s",
          groupByField, inputSchema));
      }
      fields.add(fieldSchema);
    }
    return Schema.recordOf("group.key.schema", fields);
  }
}
