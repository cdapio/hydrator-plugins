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

package com.guavus.featureengineering.cdap.plugin.batch.joiner;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.batch.BatchJoiner;

import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.guavus.featureengineering.cdap.plugin.batch.joiner.config.JoinerConfigDynamic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("JoinerDynamic")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are "
        + "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs "
        + "and records from non-required inputs will only be present if they match join criteria. If there are no "
        + "required inputs, outer join will be performed")
public class JoinerDynamic extends Joiner {
    private final JoinerConfigDynamic conf;

    public JoinerDynamic(JoinerConfigDynamic conf) {
        super(conf);
        this.conf = conf;
    }

    @Override
    public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
        Map<String, Schema> inputSchema = getInputSchemaFromJoinRow(joinRow);
        StructuredRecord.Builder outRecordBuilder = StructuredRecord.builder(getOutputSchema(inputSchema));
        Set<String> schemaSet = new HashSet<String>();
        for (JoinElement<StructuredRecord> joinElement : joinRow) {
            String stageName = joinElement.getStageName();
            StructuredRecord record = joinElement.getInputRecord();
            Schema schema = inputSchema.get(stageName);
            Map<String, String> selectedFields = perStageSelectedFields.row(stageName);
            if (selectedFields != null && !selectedFields.isEmpty()) {
                for (Map.Entry<String, String> entry : selectedFields.entrySet()) {
                    String selectedFieldName = entry.getKey();
                    boolean added = false;
                    if (schema.getField(selectedFieldName) == null) {
                        List<Field> matchingFields = getAllMatchingSchemaFields(record.getSchema().getFields(),
                                selectedFieldName);
                        for (Field matchingField : matchingFields) {
                            String outputFieldName = stageName + "_" + matchingField.getName() + "_";
                            outRecordBuilder.set(outputFieldName,
                                    convertNANToZero(record.get(matchingField.getName())));
                            added = true;
                        }
                    }
                    if (added) {
                        continue;
                    }
                    outRecordBuilder.set(entry.getValue(), convertNANToZero(record.get(selectedFieldName)));
                }
                continue;
            }
            Set<String> joinKeys = new HashSet<String>(this.perStageJoinKeys.get(stageName));
            String suffixToBeAdded = this.keysToBeAppendedMap.get(stageName);
            if (suffixToBeAdded == null) {
                suffixToBeAdded = "";
            }
            for (Schema.Field field : record.getSchema().getFields()) {
                String inputFieldName = field.getName();

                // drop the field if not part of selectedFields config input.
                if (selectedFields != null && !selectedFields.isEmpty()
                        && !selectedFields.containsKey(inputFieldName)) {
                    continue;
                }
                String outputFieldName = "";
                // get output field from selectedFields config.
                if (selectedFields != null && !selectedFields.isEmpty()) {
                    outputFieldName = selectedFields.get(inputFieldName);
                } else {
                    outputFieldName = inputFieldName;
                }
                if (!joinKeys.contains(inputFieldName)) {
                    outputFieldName += suffixToBeAdded;
                }
                boolean result = schemaSet.add(outputFieldName);
                try {
                    if (result) {
                        outRecordBuilder.set(outputFieldName, convertNANToZero(record.get(inputFieldName)));
                    }
                } catch (Throwable th) {
                    // consuming this exception as this is expected behavior.
                }
            }
        }
        return outRecordBuilder.build();
    }

    private Map<String, Schema> getInputSchemaFromJoinRow(Iterable<JoinElement<StructuredRecord>> joinRow) {
        Map<String, Map<String, Field>> inputSchemaFields = new HashMap<>();
        for (JoinElement<StructuredRecord> joinElement : joinRow) {
            String stageName = joinElement.getStageName();
            Set<String> joinKeys = new HashSet<String>(this.perStageJoinKeys.get(stageName));
            StructuredRecord record = joinElement.getInputRecord();
            Map<String, Field> fieldMap = inputSchemaFields.get(stageName);
            if (fieldMap == null) {
                fieldMap = new HashMap<>();
                inputSchemaFields.put(stageName, fieldMap);
            }
            for (Field field : record.getSchema().getFields()) {
                if (fieldMap.containsKey(field.getName())) {
                    continue;
                }
                fieldMap.put(field.getName(), Schema.Field.of(field.getName(), field.getSchema()));
            }
        }
        Map<String, Schema> inputSchema = new HashMap<String, Schema>();
        for (JoinElement<StructuredRecord> joinElement : joinRow) {
            String stageName = joinElement.getStageName();
            StructuredRecord record = joinElement.getInputRecord();
            if (inputSchema.containsKey(stageName)) {
                continue;
            }
            inputSchema.put(stageName,
                    Schema.recordOf(record.getSchema().getRecordName(), inputSchemaFields.get(stageName).values()));
        }
        return inputSchema;
    }

    private List<Field> getAllMatchingSchemaFields(List<Field> fields, String fieldName) {
        List<Field> matchingFields = new LinkedList<Field>();
        for (Field field : fields) {
            if (field.getName().contains(fieldName)) {
                matchingFields.add(field);
            }
        }
        return matchingFields;
    }

    @Override
    protected Schema getOutputSchema(Map<String, Schema> inputSchemas) {
        if (inputSchemas == null) {
            return null;
        }
        validateRequiredInputs(inputSchemas);

        // stage name to input schema
        Map<String, Schema> inputs = new HashMap<>(inputSchemas);
        // Selected Field name to output field info
        Map<String, OutputFieldInfo> outputFieldInfo = new LinkedHashMap<>();
        List<String> duplicateAliases = new ArrayList<>();

        // order of fields in output schema will be same as order of selectedFields
        Set<Table.Cell<String, String, String>> rows = perStageSelectedFields.cellSet();

        Set<String> addedStages = new HashSet<String>();

        addColumsForSelectedFields(rows, outputFieldInfo, addedStages, inputs, duplicateAliases);
        Set<String> joinKeys = new HashSet<String>();
        addColumnsForRemainingInputStages(inputs, outputFieldInfo, duplicateAliases, joinKeys, addedStages);

        return Schema.recordOf("joinDyn.output", getOutputFields(outputFieldInfo, joinKeys));
    }

    private List<Field> getOutputFields(Map<String, OutputFieldInfo> outputFieldInfo, Set<String> joinKeys) {
        List<Schema.Field> outputFields = new ArrayList<>();
        for (String key : joinKeys) {
            outputFields.add(outputFieldInfo.get(key).getField());
            outputFieldInfo.remove(key);
        }
        for (OutputFieldInfo fieldInfo : outputFieldInfo.values()) {
            outputFields.add(fieldInfo.getField());
        }
        return outputFields;
    }

    private void addColumnsForRemainingInputStages(Map<String, Schema> inputs,
            Map<String, OutputFieldInfo> outputFieldInfo, List<String> duplicateAliases, Set<String> joinKeys2,
            Set<String> addedStages) {
        boolean addedJoinKeys = false;
        Map<String, String> stageSuffixMap = this.keysToBeAppendedMap;
        for (Map.Entry<String, Schema> entry : inputs.entrySet()) {
            String stageName = entry.getKey();
            if (addedStages.contains(stageName)) {
                continue;
            }
            Schema inputSchema = entry.getValue();
            List<String> joinKeys = this.perStageJoinKeys.get(stageName);
            String suffixToBeAppended = stageSuffixMap.get(stageName);
            if (suffixToBeAppended == null) {
                suffixToBeAppended = "";
            }
            suffixToBeAppended = suffixToBeAppended.trim();
            Set<String> joinKeySet = new HashSet<String>();
            if (joinKeys != null) {
                joinKeySet.addAll(joinKeys);
            }
            Set<String> allFieldName = new HashSet<String>();
            for (Field inputField : inputSchema.getFields()) {
                String inputFieldName = inputField.getName();
                if (addedJoinKeys && joinKeySet.contains(inputFieldName)) {
                    continue;
                }

                String alias = inputFieldName;
                if (!joinKeySet.contains(inputFieldName)) {
                    alias += suffixToBeAppended;
                }
                allFieldName.add(inputFieldName);

                if (outputFieldInfo.containsKey(alias)) {
                    OutputFieldInfo outInfo = outputFieldInfo.get(alias);
                    if (duplicateAliases.add(alias)) {
                        duplicateFields.put(outInfo.getStageName(), outInfo.getInputFieldName());
                    }
                    duplicateFields.put(stageName, inputFieldName);
                    continue;
                }

                if (inputField == null) {
                    throw new IllegalArgumentException(
                            String.format("Invalid field: %s of stage '%s' does not exist in input schema %s.",
                                    inputFieldName, stageName, inputSchema));
                }
                // set nullable fields for non-required inputs
                if (requiredInputs.contains(stageName) || inputField.getSchema().isNullable()) {
                    outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                            Schema.Field.of(alias, inputField.getSchema())));
                } else {
                    outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                            Schema.Field.of(alias, Schema.nullableOf(inputField.getSchema()))));
                }
            }
            if (allFieldName.containsAll(joinKeySet)) {
                addedJoinKeys = true;
                joinKeys2.clear();
                joinKeys2.addAll(joinKeySet);
            }
        }

    }

    private void addColumsForSelectedFields(Set<Cell<String, String, String>> rows,
            Map<String, OutputFieldInfo> outputFieldInfo, Set<String> addedStages, Map<String, Schema> inputs,
            List<String> duplicateAliases) {
        for (Table.Cell<String, String, String> row : rows) {
            String stageName = row.getRowKey();
            String inputFieldName = row.getColumnKey();
            String alias = row.getValue();
            Schema inputSchema = inputs.get(stageName);
            if (inputSchema == null) {
                throw new IllegalArgumentException(
                        String.format("Input schema for input stage %s can not be null", stageName));
            }
            addedStages.add(stageName);
            boolean added = false;
            if (inputSchema.getField(inputFieldName) == null) {
                List<Field> matchingFields = getAllMatchingSchemaFields(inputSchema.getFields(), inputFieldName);
                for (Field matchingField : matchingFields) {
                    String outputFieldName = stageName + "_" + matchingField.getName() + "_";

                    // set nullable fields for non-required inputs
                    if (requiredInputs.contains(stageName) || matchingField.getSchema().isNullable()) {
                        outputFieldInfo.put(outputFieldName, new OutputFieldInfo(outputFieldName, stageName,
                                outputFieldName, Schema.Field.of(outputFieldName, matchingField.getSchema())));
                    } else {
                        outputFieldInfo.put(outputFieldName, new OutputFieldInfo(outputFieldName, stageName,
                                outputFieldName,
                                Schema.Field.of(outputFieldName, Schema.nullableOf(matchingField.getSchema()))));
                    }
                    added = true;
                }
            }
            if (added) {
                continue;
            }
            if (outputFieldInfo.containsKey(alias)) {
                OutputFieldInfo outInfo = outputFieldInfo.get(alias);
                if (duplicateAliases.add(alias)) {
                    duplicateFields.put(outInfo.getStageName(), outInfo.getInputFieldName());
                }
                duplicateFields.put(stageName, inputFieldName);
                continue;
            }

            Schema.Field inputField = inputSchema.getField(inputFieldName);
            if (inputField == null) {
                throw new IllegalArgumentException(
                        String.format("Invalid field: %s of stage '%s' does not exist in input schema %s.",
                                inputFieldName, stageName, inputSchema));
            }
            // set nullable fields for non-required inputs
            if (requiredInputs.contains(stageName) || inputField.getSchema().isNullable()) {
                outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                        Schema.Field.of(alias, inputField.getSchema())));
            } else {
                outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                        Schema.Field.of(alias, Schema.nullableOf(inputField.getSchema()))));
            }
        }
    }

}
