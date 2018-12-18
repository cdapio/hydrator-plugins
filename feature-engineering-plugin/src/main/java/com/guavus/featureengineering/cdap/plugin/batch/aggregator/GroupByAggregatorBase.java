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

package com.guavus.featureengineering.cdap.plugin.batch.aggregator;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.StructuredRecord.Builder;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.hydrator.plugin.batch.aggregator.RecordAggregator;
import co.cask.hydrator.plugin.batch.aggregator.function.AggregateFunction;

import com.guavus.featureengineering.cdap.plugin.batch.aggregator.config.GroupByConfigBase;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.config.GroupByConfigBase.FunctionInfo;
import com.guavus.featureengineering.cdap.plugin.batch.aggregator.config.GroupByConfigCategorical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Batch group by aggregator.
 */
public abstract class GroupByAggregatorBase extends RecordAggregator {
    private final GroupByConfigBase conf;
    private List<String> groupByFields;
    private List<GroupByConfigBase.FunctionInfo> functionInfos;
    private Map<String, List<String>> categoricalDictionaryMap;
    private Map<String, AggregateFunction> aggregateFunctions;
    private Boolean isDynamicSchema;

    private Map<String, GroupByConfigBase.FunctionInfo> functionInfoMap;

    public GroupByAggregatorBase(GroupByConfigBase conf) {
        super(conf.numPartitions);
        this.conf = conf;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        List<String> groupByFields = conf.getGroupByFields();
        List<GroupByConfigBase.FunctionInfo> aggregates = conf.getAggregates();
        Map<String, List<String>> categoricalDictionaryMap = null;
        if (conf instanceof GroupByConfigCategorical) {
            categoricalDictionaryMap = ((GroupByConfigCategorical) conf).getCategoricalDictionaryMap();
        }

        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        Schema inputSchema = stageConfigurer.getInputSchema();
        // if null, the input schema is unknown, or its multiple schemas.
        if (inputSchema == null || conf.isDynamicSchema) {
            stageConfigurer.setOutputSchema(null);
            return;
        }

        // otherwise, we have a constant input schema. Get the output schema and
        // propagate the schema, which is group by fields + aggregate fields
        stageConfigurer.setOutputSchema(
                getStaticOutputSchema(inputSchema, groupByFields, aggregates, categoricalDictionaryMap, null, null));
    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        groupByFields = conf.getGroupByFields();
        functionInfos = conf.getAggregates();
        categoricalDictionaryMap = conf.getCategoricalDictionaryMap();
        isDynamicSchema = conf.isDynamicSchema;
    }

    @Override
    public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
        // app should provide some way to make some data calculated in configurePipeline
        // available here.
        // then we wouldn't have to calculate schema here
        StructuredRecord.Builder builder = StructuredRecord.builder(getGroupKeySchema(record.getSchema()));
        for (String groupByField : conf.getGroupByFields()) {
            builder.set(groupByField, record.get(groupByField));
        }
        emitter.emit(builder.build());
    }

    @Override
    public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
            Emitter<StructuredRecord> emitter) throws Exception {
        if (!iterator.hasNext()) {
            return;
        }
        StructuredRecord firstVal = iterator.next();
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        List<StructuredRecord> inputColumnList = new LinkedList<StructuredRecord>();
        updateSchema(fieldMap, inputColumnList, firstVal);
        while (iterator.hasNext()) {
            updateSchema(fieldMap, inputColumnList, iterator.next());
        }
        Schema inputSchema = Schema.recordOf(firstVal.getSchema().getRecordName(), fieldMap.values());

        Schema outputSchema = initAggregates(inputSchema);
        updateAggregates(inputColumnList);

        if (isDynamicSchema) {
            Schema dynamicOutoutSchema = getDynamicOutputSchema(inputSchema, conf.getGroupByFields(),
                    conf.getAggregates(), aggregateFunctions);
            if (dynamicOutoutSchema != null) {
                outputSchema = dynamicOutoutSchema;
            }
        }
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (String groupByField : groupByFields) {
            builder.set(groupByField, groupKey.get(groupByField));
        }
        emitOutputValues(builder, emitter);
    }

    protected Schema getDynamicOutputSchema(Schema inputSchema, List<String> groupByFields,
            List<GroupByConfigBase.FunctionInfo> aggregates, Map<String, AggregateFunction> aggregateFunctions) {
        // Check that all the group by fields exist in the input schema,
        List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
        for (String groupByField : groupByFields) {
            Schema.Field field = inputSchema.getField(groupByField);
            if (field == null) {
                continue;
            }
            outputFields.add(field);
        }

        // check that all fields needed by aggregate functions exist in the input
        // schema.
        for (GroupByConfigBase.FunctionInfo functionInfo : aggregates) {
            for (String field : functionInfo.getField()) {
                if (field == null) {
                    throw new IllegalArgumentException("Input Argument field can't be null");
                }
                if (field.equalsIgnoreCase(functionInfo.getName())) {
                    throw new IllegalArgumentException(String.format(
                            "Name '%s' should not be same as aggregate field '%s'", functionInfo.getName(), field));
                }
            }

            AggregateFunction aggregateFunction = aggregateFunctions.get(functionInfo.getName());
            if (aggregateFunction.getAggregate() instanceof Map) {
                if (functionInfoMap != null) {
                    functionInfoMap.put(functionInfo.getName(), functionInfo);
                }
                Map<String, Object> valueSpecificFrequencyCount = (Map<String, Object>) aggregateFunction
                        .getAggregate();
                for (String dict2 : valueSpecificFrequencyCount.keySet()) {
                    Object result = valueSpecificFrequencyCount.get(dict2);
                    if (result instanceof Map) {
                        Map<String, Integer> frequencyCount = (Map<String, Integer>) result;
                        for (String dict1 : frequencyCount.keySet()) {
                            outputFields.add(Schema.Field.of(
                                    functionInfo.getName().toLowerCase() + "_" + dict1.toLowerCase() + "__"
                                            + dict2.toLowerCase(),
                                    aggregateFunction.getOutputSchema().isNullable()
                                            ? aggregateFunction.getOutputSchema()
                                            : Schema.nullableOf(aggregateFunction.getOutputSchema())));
                        }
                    } else {
                        Map<String, Integer> frequencyCount = (Map<String, Integer>) aggregateFunction.getAggregate();
                        if (frequencyCount != null) {
                            for (String token : frequencyCount.keySet()) {
                                outputFields.add(Schema.Field.of(functionInfo.getName() + "_" + token.toLowerCase(),
                                        aggregateFunction.getOutputSchema().isNullable()
                                                ? aggregateFunction.getOutputSchema()
                                                : Schema.nullableOf(aggregateFunction.getOutputSchema())));
                            }
                        }
                    }
                }
            } else {
                return null;
            }
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".agg", outputFields);
    }

    protected List<Field> getAllMatchingSchemaFields(List<Field> fields, String fieldName) {
        List<Field> matchingFields = new LinkedList<Field>();
        for (Field field : fields) {
            if (field.getName().contains(fieldName)) {
                matchingFields.add(field);
            }
        }
        return matchingFields;
    }

    protected void emitOutputValues(Builder builder, Emitter<StructuredRecord> emitter) {
        if (!isDynamicSchema) {
            emitStaticOutputValues(builder, emitter);
        } else {
            emitDynamicOutputValue(builder, emitter);
        }
    }

    protected void emitDynamicOutputValue(Builder builder, Emitter<StructuredRecord> emitter) {
        for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
            Object result = aggregateFunction.getValue().getAggregate();
            if (result instanceof Map) {
                Map<String, Object> valueSpecificFrequencyCount = (Map<String, Object>) aggregateFunction.getValue()
                        .getAggregate();

                for (String dict2 : valueSpecificFrequencyCount.keySet()) {
                    result = valueSpecificFrequencyCount.get(dict2);
                    if (result instanceof Map) {
                        Map<String, Integer> frequencyCount = (Map<String, Integer>) valueSpecificFrequencyCount
                                .get(dict2);
                        for (String dict1 : frequencyCount.keySet()) {
                            Integer count = frequencyCount.get(dict1);
                            if (count != null) {
                                builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "__"
                                        + dict2.toLowerCase(), count);
                            } else {
                                builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict1.toLowerCase() + "__"
                                        + dict2.toLowerCase(), 0);
                            }
                        }
                    } else {
                        builder.set(aggregateFunction.getKey() + "_" + dict2.toLowerCase(),
                                valueSpecificFrequencyCount.get(dict2));
                    }
                }
            } else {
                builder.set(aggregateFunction.getKey(), aggregateFunction.getValue().getAggregate());
            }

        }
        emitter.emit(builder.build());
    }

    protected void emitStaticOutputValues(Builder builder, Emitter<StructuredRecord> emitter) {
        for (Map.Entry<String, AggregateFunction> aggregateFunction : aggregateFunctions.entrySet()) {
            if (functionInfoMap != null && !functionInfoMap.isEmpty() && !categoricalDictionaryMap.isEmpty()
                    && categoricalDictionaryMap != null) {
                FunctionInfo functionInfo = functionInfoMap.get(aggregateFunction.getKey());
                List<String> dictionary = categoricalDictionaryMap.get(functionInfo.getName());

                Map<String, Object> valueSpecificFrequencyCount = (Map<String, Object>) aggregateFunction.getValue()
                        .getAggregate();
                for (String dict : dictionary) {
                    if (dict.contains("__")) {
                        String token[] = dict.trim().split("__");
                        if (valueSpecificFrequencyCount.containsKey(token[1])) {
                            Map<String, Integer> frequencyCount = (Map<String, Integer>) valueSpecificFrequencyCount
                                    .get(token[1]);
                            Integer count = frequencyCount.get(token[0]);
                            if (count != null) {
                                builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), count);
                            } else {
                                builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), 0);
                            }
                        } else {
                            builder.set(aggregateFunction.getKey().toLowerCase() + "_" + dict.toLowerCase(), 0);
                        }
                    } else {
                        if (valueSpecificFrequencyCount.containsKey(dict.toLowerCase())) {
                            builder.set(aggregateFunction.getKey() + "_" + dict.toLowerCase(),
                                    (Integer) valueSpecificFrequencyCount.get(dict));
                        } else {
                            builder.set(aggregateFunction.getKey() + "_" + dict.toLowerCase(), 0);
                        }
                    }
                }
            } else {
                builder.set(aggregateFunction.getKey(), aggregateFunction.getValue().getAggregate());
            }
        }
        emitter.emit(builder.build());
    }

    protected Schema getStaticOutputSchema(Schema inputSchema, List<String> groupByFields,
            List<GroupByConfigBase.FunctionInfo> aggregates, Map<String, List<String>> categoricalDictionaryMap,
            Map<String, AggregateFunction> aggregateFunctions, Map<String, FunctionInfo> functionInfoMap) {
        // Check that all the group by fields exist in the input schema,
        List<Schema.Field> outputFields = new ArrayList<>(groupByFields.size() + aggregates.size());
        for (String groupByField : groupByFields) {
            Schema.Field field = inputSchema.getField(groupByField);
            if (field == null) {
                throw new IllegalArgumentException(
                        String.format("Cannot group by field '%s' because it does not exist in input schema %s.",
                                groupByField, inputSchema));
            }
            outputFields.add(field);
        }

        // check that all fields needed by aggregate functions exist in the input
        // schema.
        for (GroupByConfigBase.FunctionInfo functionInfo : aggregates) {
            // special case count(*) because we don't have to check that the input field
            // exists
            if (functionInfo.getField().length == 1 && functionInfo.getField()[0].equals("*")) {
                AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(null);
                outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
                if (aggregateFunctions != null) {
                    aggregateFunction.beginFunction();
                    aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
                }
                continue;
            }

            String[] fields = functionInfo.getField();
            Schema[] inputFieldSchema = new Schema[fields.length];
            int index = 0;
            for (String field : fields) {
                if (field == null) {
                    throw new IllegalArgumentException(String.format(
                            "Invalid aggregate %s(%s): Field '%s' does not exist in input schema %s.",
                            functionInfo.getFunction(), functionInfo.getField(), functionInfo.getField(), inputSchema));
                }
                if (field.equalsIgnoreCase(functionInfo.getName())) {
                    throw new IllegalArgumentException(String.format(
                            "Name '%s' should not be same as aggregate field '%s'", functionInfo.getName(), field));
                }
                inputFieldSchema[index++] = inputSchema.getField(field).getSchema();
            }

            AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputFieldSchema);
            if (aggregateFunctions != null) {
                aggregateFunction.beginFunction();
                aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
            }
            List<String> dictionary = new LinkedList<String>();

            if (categoricalDictionaryMap != null) {
                dictionary = categoricalDictionaryMap.get(functionInfo.getName());
                if (dictionary != null) {
                    if (functionInfoMap != null) {
                        functionInfoMap.put(functionInfo.getName(), functionInfo);
                    }
                    for (String dict : dictionary) {
                        outputFields
                                .add(Schema.Field.of(functionInfo.getName().toLowerCase() + "_" + dict.toLowerCase(),
                                        aggregateFunction.getOutputSchema()));

                    }
                    continue;
                }
            }
            outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".agg", outputFields);
    }

    protected void updateAggregates(List<StructuredRecord> recordList) {
        for (StructuredRecord record : recordList) {
            for (AggregateFunction aggregateFunction : aggregateFunctions.values()) {
                aggregateFunction.operateOn(record);
            }
        }
    }

    protected void updateSchema(Map<String, Field> fieldMap, List<StructuredRecord> inputColumnList,
            StructuredRecord value) {
        for (Field field : value.getSchema().getFields()) {
            if (fieldMap.containsKey(field.getName())) {
                continue;
            }
            fieldMap.put(field.getName(), Schema.Field.of(field.getName(), field.getSchema()));
        }
        inputColumnList.add(value);
    }

    protected Schema initAggregates(Schema inputSchema) {
        aggregateFunctions = new LinkedHashMap<>();
        this.functionInfoMap = new HashMap<>();
        if (!isDynamicSchema) {
            return getStaticOutputSchema(inputSchema, groupByFields, functionInfos, categoricalDictionaryMap,
                    aggregateFunctions, functionInfoMap);
        } else {
            return getDynamicAggregateSchemaForNormalColumns(inputSchema);
        }
    }

    protected Schema getDynamicAggregateSchemaForNormalColumns(Schema inputSchema) {
        List<Schema.Field> outputFields = new ArrayList<>();
        for (String groupByField : groupByFields) {
            Schema.Field field = inputSchema.getField(groupByField);
            if (field != null) {
                outputFields.add(field);
            }
        }

        aggregateFunctions = new HashMap<>();
        for (GroupByConfigBase.FunctionInfo functionInfo : functionInfos) {
            if (functionInfo.getField().length == 1 && functionInfo.getField()[0].equals("*")) {
                AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(null);
                outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
                continue;
            }
            String[] fields = functionInfo.getField();
            Schema[] inputFieldSchema = new Schema[fields.length];
            int index = 0;
            for (String field : fields) {
                inputFieldSchema[index++] = inputSchema.getField(field).getSchema();
            }
            Schema.Field inputField = inputSchema.getField(fields[0]);
            Schema fieldSchema = inputField == null ? null : inputField.getSchema();
            if (fieldSchema == null) {
                // could be the case that column is categorical column and exists in extended
                // form along with dictionary in schema.
                List<Field> matchingFields = getAllMatchingSchemaFields(inputSchema.getFields(), fields[0]);
                for (Field matchingField : matchingFields) {
                    String outputFieldName = functionInfo.getFunction().toLowerCase() + "_" + matchingField.getName()
                            + "_";
                    Schema[] tempSchema = new Schema[1];
                    tempSchema[0] = matchingField.getSchema();
                    AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(tempSchema);
                    aggregateFunction.beginFunction();
                    outputFields.add(Schema.Field.of(outputFieldName, aggregateFunction.getOutputSchema()));
                    aggregateFunctions.put(outputFieldName, aggregateFunction);
                }
                continue;
            }
            AggregateFunction aggregateFunction = functionInfo.getAggregateFunction(inputFieldSchema);
            aggregateFunction.beginFunction();
            outputFields.add(Schema.Field.of(functionInfo.getName(), aggregateFunction.getOutputSchema()));
            aggregateFunctions.put(functionInfo.getName(), aggregateFunction);
        }
        return Schema.recordOf(inputSchema.getRecordName() + ".agg", outputFields);
    }

    private Schema getGroupKeySchema(Schema inputSchema) {
        List<Schema.Field> fields = new ArrayList<>();
        for (String groupByField : conf.getGroupByFields()) {
            Schema.Field fieldSchema = inputSchema.getField(groupByField);
            if (fieldSchema == null) {
                throw new IllegalArgumentException(
                        String.format("Cannot group by field '%s' because it does not exist in input schema %s",
                                groupByField, inputSchema));
            }
            fields.add(fieldSchema);
        }
        return Schema.recordOf("group.key.schema", fields);
    }

}
