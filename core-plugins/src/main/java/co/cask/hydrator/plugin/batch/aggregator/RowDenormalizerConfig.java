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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Config for RowDenormalizer Aggregator Plugin.
 */
public class RowDenormalizerConfig extends AggregatorConfig {

  @Description("Name of the column in the input record which will be used to group the raw data. For Example, " +
    "KeyField.")
  private final String keyField;

  @Description("Name of the column in the input record which contains the names of output schema columns. " +
    "For example, " +
    "input records have columns 'KeyField', 'FieldName', 'FieldValue'. " +
    "'FieldName' contains 'FirstName', 'LastName', 'Address'. " +
    "So the output record will have column names as 'FirstName', 'LastName', 'Address'.")
  private final String fieldName;

  @Description("Name of the column in the input record which contains the values for output schema columns. " +
    "For example, " +
    "input records have columns 'KeyField', 'FieldName', 'FieldValue' " +
    "and the 'FieldValue' column contains 'John', 'Wagh', 'NE Lakeside'. " +
    "So the output record will have values for columns 'FirstName', 'LastName', 'Address' as 'John', 'Wagh', 'NE " +
    "Lakeside' respectively.")
  private final String fieldValue;

  @Description("List of the output fields with its alias to be included in denormalized output. This is a comma " +
    "separated list of key-value pairs, where key-value pairs are separated by a colon ':'. For example, " +
    "Firstname:fname,Lastname:lname,Address:addr")
  private final String outputFields;

  @VisibleForTesting
  RowDenormalizerConfig(String keyField, String fieldName, String fieldValue, String outputFields) {
    this.keyField = keyField;
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.outputFields = outputFields;
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
   * Returns name of the input field 'FieldName' given as input by user.
   *
   * @return field name
   */
  String getFieldName() {
    return fieldName;
  }

  /**
   * Returns name of the input field 'FieldValue' given as input by user.
   *
   * @return field value
   */
  String getFieldValue() {
    return fieldValue;
  }

  /**
   * Fetches the output fields (or output field alias, if provided) to build the output schema.
   *
   * @return List of output fields
   */
  Set<String> getOutputFields() {
    Set<String> fields = new HashSet<String>();
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
   * Creates a map for output field and its alias, if present.
   *
   * @return Map of output fields and its alias
   */
  Map<String, String> getOutputFieldMappings() {
    Map<String, String> outputFieldMappings = new HashMap<String, String>();
    for (String field : Splitter.on(',').trimResults().split(outputFields)) {
      String[] value = field.split(":");
      if (value.length == 2) {
        outputFieldMappings.put(value[0], value[1]);
      }
    }
    return outputFieldMappings;
  }

  /**
   * Validates the config properties set by the user.
   */
  void validate() {
    if (keyField.isEmpty()) {
      throw new IllegalArgumentException("The 'keyField' property must be set.");
    } else if (fieldName.isEmpty()) {
      throw new IllegalArgumentException("The 'fieldName' property must be set.");
    } else if (fieldValue.isEmpty()) {
      throw new IllegalArgumentException("The 'fieldValue' property must be set.");
    } else if (outputFields.isEmpty()) {
      throw new IllegalArgumentException("The 'outputFields' property must be set.");
    }
  }
}
