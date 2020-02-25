/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utility class for recording field-level lineage for transform operations.
 */
public final class TransformLineageRecorderUtils {

  public static final String IDENTITY_TRANSFORM_DESCRIPTION = "Unchanged fields in the transform.";
  public static final String DROP_TRANSFORM_DESCRIPTION = "Dropped fields not included in the output.";

  private TransformLineageRecorderUtils() {

  }

  /**
   * Returns the list of fields as a list of strings.
   * @param schema input or output schema
   * @return String list of fields from the schema
   */
  public static List<String> getFields(@Nullable Schema schema) {
    if (schema == null || schema.getFields() == null || schema.getFields().isEmpty()) {
      return Collections.emptyList();
    }

    return schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  /**
   * Use the list of input fields to generate a one-to-one on the same list.
   * @param input a list of input fields
   * @param name prefix of name of each operation. Real name is name_inputField
   * @param description Description to be used for every operation in this transform list
   * @return list of operations where each is just input(i) -> input(i)
   */
  public static List<FieldOperation> generateOneToOnes(List<String> input, String name, String description) {
    return input.stream()
      .map(inputField -> new FieldTransformOperation(name + "_" + inputField, description,
        Collections.singletonList(inputField),
        Collections.singletonList(inputField)))
      .collect(Collectors.toList());
  }

  /**
   * Use the list of input fields to generate a list of drop operations.
   * @param input a list of input fields
   * @return list of operations where each is just input(i) -> []
   */
  public static List<FieldOperation> generateDrops(List<String> input) {
    return input.stream()
      .map(inputField -> new FieldTransformOperation("drop_" + inputField, DROP_TRANSFORM_DESCRIPTION,
        Collections.singletonList(inputField),
        Collections.emptyList()))
      .collect(Collectors.toList());
  }

  /**
   * Return a single operation with all input mappings to the single output.
   * @param input list of input fields
   * @param output single output field
   * @param name name of operation
   * @param description complete sentence description of operation
   * @return the FieldOperation as a list
   */
  public static List<FieldOperation> generateManyToOne(List<String> input, String output, String name,
    String description) {
    return Collections.singletonList(new FieldTransformOperation(name, description, input, output));
  }

  /**
   * Return a single operation with the input mapping to all outputs.
   * @param input single input field
   * @param output list of output fields
   * @param name name of operation
   * @param description complete sentence description of operation
   * @return the FieldOperation as a list
   */
  public static List<FieldOperation> generateOneToMany(String input, List<String> output, String name,
    String description) {
    return Collections.singletonList(new FieldTransformOperation(name, description,
                                                                 Collections.singletonList(input), output));
  }

  /**
   * Returns a single operation mapping all inputs to all outputs.
   * @param input list of input fields
   * @param output list of input fields
   * @param name name of operation
   * @param description complete sentence description of operation
   * @return the FieldOperation as a list
   */
  public static List<FieldOperation> generateManyToMany(List<String> input, List<String> output, String name,
    String description) {
    return Collections.singletonList(new FieldTransformOperation(name, description, input, output));
  }

  /**
   * Returns a single operation mapping the input to the output.
   * @param input single input field
   * @param output single output fields
   * @param name name of operation
   * @param description complete sentence description of operation
   * @return the FieldOperation as a list
   */
  public static List<FieldOperation> generateOneToOne(String input, String output, String name, String description) {
    return Collections.singletonList(new FieldTransformOperation(name, description,
                                                                 Collections.singletonList(input), output));
  }
}
