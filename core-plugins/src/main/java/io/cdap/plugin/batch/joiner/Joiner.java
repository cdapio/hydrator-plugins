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

package io.cdap.plugin.batch.joiner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.JoinConfig;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Joiner")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are " +
  "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs and " +
  "records from non-required inputs will only be present if they match join criteria. If there are no required " +
  "inputs, outer join will be performed")
public class Joiner extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {

  public static final String JOIN_OPERATION_DESCRIPTION = "Used as a key in a join";
  public static final String IDENTITY_OPERATION_DESCRIPTION = "Unchanged as part of a join";
  public static final String RENAME_OPERATION_DESCRIPTION = "Renamed as a part of a join";

  private final JoinerConfig conf;
  private Schema outputSchema;
  private Map<String, List<String>> perStageJoinKeys;
  private Table<String, String, String> perStageSelectedFields;
  private Set<String> requiredInputs;
  private Multimap<String, String> duplicateFields = ArrayListMultimap.create();

  public Joiner(JoinerConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
    MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    FailureCollector collector = pipelineConfigurer.getMultiInputStageConfigurer().getFailureCollector();
    init(inputSchemas, collector);
    collector.getOrThrowException();
    //validate the input schema and get the output schema for it
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchemas, collector));
  }

  @Override
  public void prepareRun(BatchJoinerContext context) {
    if (conf.getNumPartitions() != null) {
      context.setNumPartitions(conf.getNumPartitions());
    }
    FailureCollector collector = context.getFailureCollector();
    init(context.getInputSchemas(), collector);
    collector.getOrThrowException();
    Collection<OutputFieldInfo> outputFieldInfos = createOutputFieldInfos(context.getInputSchemas(), collector);
    collector.getOrThrowException();
    context.record(createFieldOperations(outputFieldInfos, perStageJoinKeys));
  }

  /**
   * Create the field operations from the provided OutputFieldInfo instances and join keys.
   * For join we record several types of transformation; Join, Identity, and Rename.
   * For each of these transformations, if the input field is directly coming from the schema
   * of one of the stage, the field is added as {@code stage_name.field_name}. We keep track of fields
   * outputted by operation (in {@code outputsSoFar set}, so that any operation uses that field as
   * input later, we add it without the stage name.
   *
   * Join transform operation is added with join keys as input tagged with the stage name, and join keys
   * without stage name as output.
   *
   * For other fields which are not renamed in join, Identity transform is added, while for fields which
   * are renamed Rename transform is added.
   *
   * @param outputFieldInfos collection of output fields along with information such as stage name, alias
   * @param perStageJoinKeys join keys
   * @return List of field operations
   */
  @VisibleForTesting
  public static List<FieldOperation> createFieldOperations(Collection<OutputFieldInfo> outputFieldInfos,
                                                           Map<String, List<String>> perStageJoinKeys) {
    LinkedList<FieldOperation> operations = new LinkedList<>();

    // Add JOIN operation
    List<String> joinInputs = new ArrayList<>();
    Set<String> joinOutputs = new LinkedHashSet<>();
    for (Map.Entry<String, List<String>> joinKey : perStageJoinKeys.entrySet()) {
      for (String field : joinKey.getValue()) {
        joinInputs.add(joinKey.getKey() + "." + field);
        joinOutputs.add(field);
      }
    }
    FieldOperation joinOperation = new FieldTransformOperation("Join", JOIN_OPERATION_DESCRIPTION, joinInputs,
                                                               new ArrayList<>(joinOutputs));
    operations.add(joinOperation);

    Set<String> outputsSoFar = new HashSet<>(joinOutputs);

    for (OutputFieldInfo outputFieldInfo : outputFieldInfos) {
      // input field name for the operation will come in from schema if its not outputted so far
      String stagedInputField = outputsSoFar.contains(outputFieldInfo.inputFieldName) ?
        outputFieldInfo.inputFieldName : outputFieldInfo.stageName + "." + outputFieldInfo.inputFieldName;

      if (outputFieldInfo.name.equals(outputFieldInfo.inputFieldName)) {
        // Record identity transform
        if (perStageJoinKeys.get(outputFieldInfo.stageName).contains(outputFieldInfo.inputFieldName)) {
          // if the field is part of join key no need to emit the identity transform as it is already taken care
          // by join
          continue;
        }
        String operationName = String.format("Identity %s", stagedInputField);
        FieldOperation identity = new FieldTransformOperation(operationName, IDENTITY_OPERATION_DESCRIPTION,
                                                              Collections.singletonList(stagedInputField),
                                                              outputFieldInfo.name);
        operations.add(identity);
        continue;
      }

      String operationName = String.format("Rename %s", stagedInputField);

      FieldOperation transform = new FieldTransformOperation(operationName, RENAME_OPERATION_DESCRIPTION,
                                                             Collections.singletonList(stagedInputField),
                                                             outputFieldInfo.name);
      operations.add(transform);
    }

    return operations;
  }

  @Override
  public void initialize(BatchJoinerRuntimeContext context) {
    FailureCollector collector = context.getFailureCollector();
    init(context.getInputSchemas(), collector);
    collector.getOrThrowException();

    Map<String, Schema> inputSchemas = context.getInputSchemas();
    outputSchema = context.getOutputSchema();
  }

  @Override
  public StructuredRecord joinOn(String stageName, StructuredRecord record) {
    List<Schema.Field> fields = new ArrayList<>();
    Schema schema = record.getSchema();

    List<String> joinKeys = perStageJoinKeys.get(stageName);
    int i = 1;
    for (String joinKey : joinKeys) {
      Schema.Field joinField = Schema.Field.of(String.valueOf(i++), schema.getField(joinKey).getSchema());
      fields.add(joinField);
    }
    Schema keySchema = Schema.recordOf("join.key", fields);
    StructuredRecord.Builder keyRecordBuilder = StructuredRecord.builder(keySchema);
    i = 1;
    for (String joinKey : joinKeys) {
      keyRecordBuilder.set(String.valueOf(i++), record.get(joinKey));
    }

    return keyRecordBuilder.build();
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(requiredInputs);
  }

  @Override
  public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
    StructuredRecord.Builder outRecordBuilder = StructuredRecord.builder(outputSchema);

    for (JoinElement<StructuredRecord> joinElement : joinRow) {
      String stageName = joinElement.getStageName();
      StructuredRecord record = joinElement.getInputRecord();

      Map<String, String> selectedFields = perStageSelectedFields.row(stageName);

      for (Schema.Field field : record.getSchema().getFields()) {
        String inputFieldName = field.getName();

        // drop the field if not part of fieldsToRename
        if (!selectedFields.containsKey(inputFieldName)) {
          continue;
        }

        String outputFieldName = selectedFields.get(inputFieldName);
        outRecordBuilder.set(outputFieldName, record.get(inputFieldName));
      }
    }
    return outRecordBuilder.build();
  }

  void init(Map<String, Schema> inputSchemas, FailureCollector collector) {
    validateJoinKeySchemas(inputSchemas, conf.getPerStageJoinKeys(), collector);
    requiredInputs = conf.getInputs();
    perStageSelectedFields = conf.getPerStageSelectedFields();
  }

  void validateJoinKeySchemas(Map<String, Schema> inputSchemas, Map<String, List<String>> joinKeys,
                              FailureCollector collector) {
    perStageJoinKeys = joinKeys;

    if (perStageJoinKeys.size() != inputSchemas.size()) {
      collector.addFailure("There should be join keys present from each stage.",
                           "Ensure join keys are present from each stage.")
        .withConfigProperty(conf.JOIN_KEYS);
    }

    List<Schema> prevSchemaList = null;
    for (Map.Entry<String, List<String>> entry : perStageJoinKeys.entrySet()) {
      ArrayList<Schema> schemaList = new ArrayList<>();
      String stageName = entry.getKey();

      Schema schema = inputSchemas.get(stageName);
      if (schema == null) {
        collector.addFailure(String.format("Input schema for input stage '%s' cannot be null.", stageName), null);
        collector.getOrThrowException();
      }

      for (String joinKey : entry.getValue()) {
        Schema.Field field = schema.getField(joinKey);
        if (field == null) {
          collector.addFailure(
            String.format("Join key field '%s' is not present in input stage of '%s'.", joinKey, stageName), null)
            .withConfigProperty(conf.JOIN_KEYS);
        }
        schemaList.add(field.getSchema());
      }
      if (prevSchemaList != null && !prevSchemaList.equals(schemaList)) {
        collector.addFailure(
          String.format("For stage '%s', Schemas of join keys '%s' are expected to be: '%s', but found: '%s'.",
                          stageName, entry.getValue(), prevSchemaList.toString(), schemaList.toString()), null)
          .withConfigProperty(conf.JOIN_KEYS);
      }
      prevSchemaList = schemaList;
    }
  }

  Schema getOutputSchema(Map<String, Schema> inputSchemas, FailureCollector collector) {
    validateJoinKeySchemas(inputSchemas, conf.getPerStageJoinKeys(), collector);
    requiredInputs = conf.getInputs();
    perStageSelectedFields = conf.getPerStageSelectedFields();
    duplicateFields = ArrayListMultimap.create();
    return Schema.recordOf("join.output", getOutputFields(createOutputFieldInfos(inputSchemas, collector)));
  }

  private Collection<OutputFieldInfo> createOutputFieldInfos(Map<String, Schema> inputSchemas,
                                                             FailureCollector collector) {
    validateRequiredInputs(inputSchemas, collector);
    collector.getOrThrowException();

    // stage name to input schema
    Map<String, Schema> inputs = new HashMap<>(inputSchemas);
    // Selected Field name to output field info
    Map<String, OutputFieldInfo> outputFieldInfo = new LinkedHashMap<>();
    List<String> duplicateAliases = new ArrayList<>();

    // order of fields in output schema will be same as order of selectedFields
    Set<Table.Cell<String, String, String>> rows = perStageSelectedFields.cellSet();
    for (Table.Cell<String, String, String> row : rows) {
      String stageName = row.getRowKey();
      String inputFieldName = row.getColumnKey();
      String alias = row.getValue();
      Schema inputSchema = inputs.get(stageName);

      if (inputSchema == null) {
        collector.addFailure(String.format("Input schema for input stage '%s' cannot be null.", stageName), null);
        collector.getOrThrowException();
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
        collector.addFailure(
          String.format("Field '%s' of stage '%s' must be present in input schema '%s'.",
                        inputFieldName, stageName, inputSchema), null)
          .withConfigElement("selectedFields",
                             String.format("%s.%s as %s", stageName, inputFieldName, alias));
      } else if (requiredInputs.contains(stageName) || inputField.getSchema().isNullable()) {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                                                       Schema.Field.of(alias, inputField.getSchema())));
      } else {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                                                       Schema.Field.of(alias,
                                                                       Schema.nullableOf(inputField.getSchema()))));
      }
    }

    if (!duplicateFields.isEmpty()) {
      collector.addFailure(String.format("Output schema must not contain duplicate fields: '%s' for aliases: '%s'.",
                                         duplicateFields, duplicateAliases), null)
        .withConfigProperty(conf.SELECT_FIELDS);
      collector.getOrThrowException();
    }

    return outputFieldInfo.values();
  }

  private List<Schema.Field> getOutputFields(Collection<OutputFieldInfo> fieldsInfo) {
    List<Schema.Field> outputFields = new ArrayList<>();
    for (OutputFieldInfo fieldInfo : fieldsInfo) {
      outputFields.add(fieldInfo.getField());
    }
    return outputFields;
  }

  /**
   * Class to hold information about output fields
   */
  @VisibleForTesting
  static class OutputFieldInfo {
    private String name;
    private String stageName;
    private String inputFieldName;
    private Schema.Field field;

    OutputFieldInfo(String name, String stageName, String inputFieldName, Schema.Field field) {
      this.name = name;
      this.stageName = stageName;
      this.inputFieldName = inputFieldName;
      this.field = field;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getStageName() {
      return stageName;
    }

    public void setStageName(String stageName) {
      this.stageName = stageName;
    }

    public String getInputFieldName() {
      return inputFieldName;
    }

    public void setInputFieldName(String inputFieldName) {
      this.inputFieldName = inputFieldName;
    }

    public Schema.Field getField() {
      return field;
    }

    public void setField(Schema.Field field) {
      this.field = field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      OutputFieldInfo that = (OutputFieldInfo) o;

      if (!name.equals(that.name)) {
        return false;
      }
      if (!stageName.equals(that.stageName)) {
        return false;
      }
      if (!inputFieldName.equals(that.inputFieldName)) {
        return false;
      }
      return field.equals(that.field);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + stageName.hashCode();
      result = 31 * result + inputFieldName.hashCode();
      result = 31 * result + field.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "OutputFieldInfo{" +
        "name='" + name + '\'' +
        ", stageName='" + stageName + '\'' +
        ", inputFieldName='" + inputFieldName + '\'' +
        ", field=" + field +
        '}';
    }
  }

  private void validateRequiredInputs(Map<String, Schema> inputSchemas, FailureCollector collector) {
    for (String requiredInput : requiredInputs) {
      if (!inputSchemas.containsKey(requiredInput)) {
        collector.addFailure(String.format("Provided input '%s' must be an input stage name.", requiredInput), null)
          .withConfigElement(conf.REQUIRED_INPUTS, requiredInput);
      }
    }
  }
}
