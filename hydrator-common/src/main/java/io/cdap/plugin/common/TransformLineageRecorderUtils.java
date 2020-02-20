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
  private TransformLineageRecorderUtils() {

  }

  public static List<String> getFields(@Nullable Schema schema) {
    if (schema == null || schema.getFields() == null || schema.getFields().isEmpty()) {
      return Collections.emptyList();
    }

    return schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  public static List<FieldOperation> oneToOneIn(List<String> input, String name, String description) {
    return input.stream()
                .map(inputField -> new FieldTransformOperation(name, description,
                                                               Collections.singletonList(inputField),
                                                               Collections.singletonList(inputField)))
                .collect(Collectors.toList());
  }

  public static List<FieldOperation> allInToOneOut(List<String> input, String output, String name, String description) {
    return Collections
      .singletonList(new FieldTransformOperation(name, description, input, output));
  }

  public static List<FieldOperation> oneInToAllOut(String input, List<String> output, String name, String description) {
    return Collections.singletonList(new FieldTransformOperation(name, description,
                                                                 Collections.singletonList(input), output));
  }

  public static List<FieldOperation> allInToAllOut(List<String> input, List<String> output, String name, String description) {
    return Collections
      .singletonList(new FieldTransformOperation(name, description, input, output));
  }

  public static List<FieldOperation> oneInToOneOut(String input, String output, String name, String description) {
    return Collections
      .singletonList(new FieldTransformOperation(name, description, Collections.singletonList(input), output));
  }
}
