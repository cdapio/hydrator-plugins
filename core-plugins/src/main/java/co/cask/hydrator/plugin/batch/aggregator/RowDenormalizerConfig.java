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
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config for RowDenormalizer Aggregator Plugin
 */
public class RowDenormalizerConfig extends AggregatorConfig {

  public static final String CHECK_ALIAS = "NO_ALIAS";
  @Description("Key Field which will be used to de-normalize the raw data.")
  private final String keyField;
  @Description("Name of the field in input record that contains output fields to be included in denormalized output.")
  private final String fieldName;
  @Description("Name of the field in input record that contains values for output fields to be included " +
    "in denormalized output.")
  private final String fieldValue;
  @Description("List of the output fields with its alias to be included in denormalized ouptut.")
  private final String outputFields;
  @Name("schema")
  @Nullable
  private final String schema;

  @VisibleForTesting
  RowDenormalizerConfig(String keyField, String fieldName, String fieldValue, String outputFields, String schema) {
    this.keyField = keyField;
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.outputFields = outputFields;
    this.schema = schema;
  }

  /**
   * Returns name of the keyfield given as input by user.
   *
   * @return key field
   */
  String getKeyField() {
    return keyField;
  }

  /**
   * Returns name of the input field 'FieldName' given as input by user
   *
   * @return field name
   */
  String getFieldName() {
    return fieldName;
  }

  /**
   * Returns name of the input field 'FieldValue' given as input by user
   *
   * @return field value
   */
  String getFieldValue() {
    return fieldValue;
  }

  /**
   * Fetches the output fields or output field alias if provided, to build the output schema.
   *
   * @return List of output fields
   */
  List<String> getOutputFields() {
    List<String> fields = new ArrayList<String>();
    for (String field : Splitter.on(',').trimResults().split(outputFields)) {
      String[] value = field.split(":");
      if (value.length == 1) {
        fields.add(value[0]);
      } else {
        fields.add(value[1]);
      }
    }
    return fields;
  }

  /**
   * Returns the output schema
   *
   * @return schema
   */
  String getSchema() {
    return schema;
  }

  /**
   * Creates a map for output field and its alias, if present.
   *
   * @return Map of output fields and its alias
   */
  Map<String, String> getOutputFieldMappings() {
    Map<String, String> outputFieldMappings = new HashMap<String, String>();
    for (String field : Splitter.on(',').trimResults().split(outputFields)) {
      String[] value = field.split(":");
      if (value.length == 1) {
        outputFieldMappings.put(value[0], CHECK_ALIAS);
      } else {
        outputFieldMappings.put(value[0], value[1]);
      }
    }
    return outputFieldMappings;
  }

  /**
   * Parses whether the schema json is proper or not
   *
   * @return schema
   */
  Schema parseSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  /**
   * Validates the config input coming from UI
   */
  void validate() {
    if (keyField.isEmpty()) {
      throw new IllegalArgumentException("The 'Key Field' property must be set.");
    } else if (fieldName.isEmpty()) {
      throw new IllegalArgumentException("The 'Input Field Name' property must be set.");
    } else if (fieldValue.isEmpty()) {
      throw new IllegalArgumentException("The 'Input Field Value' property must be set.");
    } else if (outputFields.isEmpty()) {
      throw new IllegalArgumentException("The 'OutputFields' property must be set.");
    }
  }
}

