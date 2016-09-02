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

package co.cask.hydrator.common;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Utility class for with methods for validating schema fields and its types,
 * validating fields are simple, checking if schema is subset of another, checking if fields are present in schema, etc
 */
public final class SchemaValidator {

  /**
   * Validate output schema fields and if input schema is present,
   * check if output schema is a subset of the input schema and return output schema.
   * @param outputSchemaString output schema from config
   * @param rowKeyField row key field from config
   * @param pipelineConfigurer Pipelineconfigurer that's used to get input schema and set output schema.
   * @return Schema - output schema
   */
  @Nullable
  public static Schema validateOutputSchemaAndInputSchemaIfPresent(String outputSchemaString, String rowKeyField,
                                                                   PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema == null && outputSchemaString == null) {
       return null;
    }

    // initialize output schema if present; otherwise, set it to input schema
    Schema outputSchema;
    if (outputSchemaString == null) {
      outputSchema = inputSchema;
    } else {
      try {
        outputSchema = Schema.parseJson(outputSchemaString);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema : " + e.getMessage(), e);
      }
    }

    // check that all fields in output schema are simple
    validateSchemaFieldsAreSimple(outputSchema);

    if (inputSchema != null) {
      // check if output schema is a subset of input schema and check if rowkey field is present in input schema
      validateOutputSchemaIsSubsetOfInputSchema(inputSchema, outputSchema);
    }
    return outputSchema;
  }

  /**
   * Checks that all the fields in output schema is part of input schema and the fields schema type matches.
   * @param inputSchema
   * @param outputSchema
   */
  public static void validateOutputSchemaIsSubsetOfInputSchema(Schema inputSchema, Schema outputSchema) {
    // check if input schema contains all the fields expected in the output schema
    for (Schema.Field field : outputSchema.getFields()) {
      if (inputSchema.getField(field.getName()) == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is present in output schema but not present in input schema",
                        field.getName()));
      } else if (!inputSchema.getField(field.getName()).getSchema().equals(field.getSchema())) {
        throw new IllegalArgumentException(
          String.format("Field type mismatch, field '%s' type in input schema is %s, " +
                          "while in output schema its of type %s", field.getName(),
                        inputSchema.getField(field.getName()).getSchema(), field.getSchema()));
      }
    }
  }

  /**
   * Iterates through the schema fields and checks their type is simple
   * @param schema
   */
  public static void validateSchemaFieldsAreSimple(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      // simple type check for fields
      if (!field.getSchema().isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is not of simple type, All fields for table sink should of simple type",
                        field.getName()));
      }
    }
  }

  /**
   * Iterates through the required fields and checks if they are present in the schema
   * @param schema
   * @param requiredFields
   */
  public static void validateFieldsArePresentInSchema(Schema schema, String... requiredFields) {
    for (String field : requiredFields) {
      if (schema.getField(field) == null) {
        throw new IllegalArgumentException(
          String.format("Field : '%s' is not present in the input schema",  field));
      }
    }
  }

  private SchemaValidator() {
    throw new AssertionError("Suppress default constructor for non-instantiability");
  }
}
