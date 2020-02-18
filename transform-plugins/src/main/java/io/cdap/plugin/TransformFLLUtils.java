/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.plugin;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Utility class for recording field-level lineage for transform operations.
 */
public class TransformFLLUtils {
  public static <C extends Collection<String>> C getInputFields(StageSubmitterContext context,
                                                                Collector<String, ?, C> collector) {
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null || inputSchema.getFields() == null || inputSchema.getFields().isEmpty()) {
      return null;
    }

    return inputSchema.getFields().stream().map(Schema.Field::getName).collect(collector);
  }

  public static <C extends Collection<String>> C getOutputFields(StageSubmitterContext context,
                                                                  Collector<String, ?, C> collector) {
    Schema outputSchema = context.getInputSchema();
    if (outputSchema == null || outputSchema.getFields() == null || outputSchema.getFields().isEmpty()) {
      return null;
    }

    return outputSchema.getFields().stream().map(Schema.Field::getName).collect(collector);
  }

  public static void oneToOneIn(StageSubmitterContext context, String namePrefix, String descriptionPrefix) {
    Set<String> input = getInputFields(context, Collectors.toSet());
    if (input == null) {
      return;
    }

    List<FieldOperation> operationList = new ArrayList<>();
    for (String inputField : input) {
      FieldTransformOperation operation =
          new FieldTransformOperation(namePrefix + inputField, descriptionPrefix + " " + inputField,
              Collections.singletonList(inputField), Collections.singletonList(inputField));
      operationList.add(operation);
    }
    context.record(operationList);
  }

  public static void allInToFirstOut(StageSubmitterContext context, String name, String description) {
    List<String> input = getInputFields(context, Collectors.toList());
    List<String> output = getOutputFields(context, Collectors.toList());
    if (input == null || output == null) {
      return;
    }

    List<FieldOperation> operations = Collections
        .singletonList(new FieldTransformOperation(name, description, input,
                                                   Collections.singletonList(output.get(0))));
    context.record(operations);
  }

  public static void firstInToAllOut(StageSubmitterContext context, String name, String description) {
    List<String> input = getInputFields(context, Collectors.toList());
    List<String> output = getOutputFields(context, Collectors.toList());
    if (input == null || output == null) {
      return;
    }

    List<FieldOperation> operations = Collections
        .singletonList(new FieldTransformOperation(name, description, Collections.singletonList(input.get(0)),
                                                   output));
    context.record(operations);
  }

  public static void allInToAllOut(StageSubmitterContext context, String name, String description) {
    List<String> input = getInputFields(context, Collectors.toList());
    List<String> output = getOutputFields(context, Collectors.toList());
    if (input == null || output == null) {
      return;
    }

    List<FieldOperation> operations = Collections
        .singletonList(new FieldTransformOperation(name, description, input, output));
    context.record(operations);
  }

  public static void firstInToFirstOut(StageSubmitterContext context, String name, String description) {
    List<String> input = getInputFields(context, Collectors.toList());
    List<String> output = getOutputFields(context, Collectors.toList());
    if (input == null || output == null) {
      return;
    }

    List<FieldOperation> operations = Collections
        .singletonList(new FieldTransformOperation(name, description, Collections.singletonList(input.get(0)),
            Collections.singletonList(output.get(0))));
    context.record(operations);
  }
}
