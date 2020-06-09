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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.InvalidJoinException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.join.error.OutputSchemaError;
import io.cdap.cdap.etl.api.join.error.SelectedFieldError;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Joiner")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are " +
  "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs and " +
  "records from non-required inputs will only be present if they match join criteria. If there are no required " +
  "inputs, outer join will be performed")
public class Joiner extends BatchAutoJoiner {

  public static final String JOIN_OPERATION_DESCRIPTION = "Used as a key in a join";
  public static final String IDENTITY_OPERATION_DESCRIPTION = "Unchanged as part of a join";
  public static final String RENAME_OPERATION_DESCRIPTION = "Renamed as a part of a join";

  private final JoinerConfig conf;

  public Joiner(JoinerConfig conf) {
    this.conf = conf;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    FailureCollector collector = context.getFailureCollector();

    boolean hasUnknownInputSchema = context.getInputStages().values().stream().anyMatch(Objects::isNull);
    if (hasUnknownInputSchema && !conf.containsMacro(JoinerConfig.OUTPUT_SCHEMA) &&
      conf.getOutputSchema(collector) == null) {
      // If input schemas are unknown, an output schema must be provided.
      collector.addFailure("Output schema must be specified", null).withConfigProperty(JoinerConfig.OUTPUT_SCHEMA);
    }

    if (conf.requiredPropertiesContainMacros()) {
      return null;
    }

    Set<String> requiredStages = conf.getRequiredInputs();
    Set<String> broadcastStages = conf.getBroadcastInputs();
    List<JoinStage> inputs = new ArrayList<>(context.getInputStages().size());
    boolean useOutputSchema = false;
    for (JoinStage joinStage : context.getInputStages().values()) {
      inputs.add(JoinStage.builder(joinStage)
        .setRequired(requiredStages.contains(joinStage.getStageName()))
        .setBroadcast(broadcastStages.contains(joinStage.getStageName()))
        .build());
      useOutputSchema = useOutputSchema || joinStage.getSchema() == null;
    }

    try {
      JoinDefinition.Builder joinBuilder = JoinDefinition.builder()
        .select(conf.getSelectedFields(collector))
        .from(inputs)
        .on(JoinCondition.onKeys()
              .setKeys(conf.getJoinKeys(collector))
              .setNullSafe(conf.isNullSafe())
              .build());
      if (useOutputSchema) {
        joinBuilder.setOutputSchema(conf.getOutputSchema(collector));
      } else {
        joinBuilder.setOutputSchemaName("join.output");
      }
      return joinBuilder.build();
    } catch (InvalidJoinException e) {
      if (e.getErrors().isEmpty()) {
        collector.addFailure(e.getMessage(), null);
      }
      for (JoinError error : e.getErrors()) {
        ValidationFailure failure = collector.addFailure(error.getMessage(), error.getCorrectiveAction());
        switch (error.getType()) {
          case JOIN_KEY:
          case JOIN_KEY_FIELD:
            failure.withConfigProperty(JoinerConfig.JOIN_KEYS);
            break;
          case SELECTED_FIELD:
            JoinField badField = ((SelectedFieldError) error).getField();
            failure.withConfigElement(
              JoinerConfig.SELECTED_FIELDS,
              String.format("%s.%s as %s", badField.getStageName(), badField.getFieldName(), badField.getAlias()));
            break;
          case OUTPUT_SCHEMA:
            OutputSchemaError schemaError = (OutputSchemaError) error;
            failure.withOutputSchemaField(schemaError.getField());
        }
      }
      throw collector.getOrThrowException();
    }
  }

  @Override
  public void prepareRun(BatchJoinerContext context) {
    if (conf.getNumPartitions() != null) {
      context.setNumPartitions(conf.getNumPartitions());
    }
    FailureCollector collector = context.getFailureCollector();
    context.record(createFieldOperations(conf.getSelectedFields(collector),
                                         conf.getJoinKeys(collector)));
  }

  /**
   * Create the field operations from the provided OutputFieldInfo instances and join keys.
   * For join we record several types of transformation; Join, Identity, and Rename.
   * For each of these transformations, if the input field is directly coming from the schema
   * of one of the stage, the field is added as {@code stage_name.field_name}. We keep track of fields
   * outputted by operation (in {@code outputsSoFar set}, so that any operation uses that field as
   * input later, we add it without the stage name.
   * <p>
   * Join transform operation is added with join keys as input tagged with the stage name, and join keys
   * without stage name as output.
   * <p>
   * For other fields which are not renamed in join, Identity transform is added, while for fields which
   * are renamed Rename transform is added.
   *
   * @param outputFields collection of output fields along with information such as stage name, alias
   * @param joinKeys join keys
   * @return List of field operations
   */
  @VisibleForTesting
  static List<FieldOperation> createFieldOperations(List<JoinField> outputFields, Set<JoinKey> joinKeys) {
    LinkedList<FieldOperation> operations = new LinkedList<>();
    Map<String, List<String>> perStageJoinKeys = joinKeys.stream()
      .collect(Collectors.toMap(JoinKey::getStageName, JoinKey::getFields));

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

    for (JoinField outputField : outputFields) {
      // input field name for the operation will come in from schema if its not outputted so far
      String stagedInputField = outputsSoFar.contains(outputField.getFieldName()) ?
        outputField.getFieldName() : outputField.getStageName() + "." + outputField.getFieldName();

      String outputFieldName = outputField.getAlias() == null ? outputField.getFieldName() : outputField.getAlias();
      if (outputField.getFieldName().equals(outputFieldName)) {
        // Record identity transform
        if (perStageJoinKeys.get(outputField.getStageName()).contains(outputField.getFieldName())) {
          // if the field is part of join key no need to emit the identity transform as it is already taken care
          // by join
          continue;
        }
        String operationName = String.format("Identity %s", stagedInputField);
        FieldOperation identity = new FieldTransformOperation(operationName, IDENTITY_OPERATION_DESCRIPTION,
                                                              Collections.singletonList(stagedInputField),
                                                              outputFieldName);
        operations.add(identity);
        continue;
      }

      String operationName = String.format("Rename %s", stagedInputField);

      FieldOperation transform = new FieldTransformOperation(operationName, RENAME_OPERATION_DESCRIPTION,
                                                             Collections.singletonList(stagedInputField),
                                                             outputFieldName);
      operations.add(transform);
    }

    return operations;
  }
}
