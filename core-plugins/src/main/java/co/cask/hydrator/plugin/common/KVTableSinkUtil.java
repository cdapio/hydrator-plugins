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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.hydrator.common.SchemaValidator;

import javax.annotation.Nullable;

public class KVTableSinkUtil {

  public static void configureKVTablePipeline(PipelineConfigurer pipelineConfigurer, KVTableSinkConfig config) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    // validate that input and output fields are present
    if (inputSchema != null) {
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, config.getKeyField());
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, config.getValueField());
      validateSchemaTypeIsStringOrBytes(inputSchema, config.getKeyField(), false);
      validateSchemaTypeIsStringOrBytes(inputSchema, config.getValueField(), true);
    }
  }

  private static void validateSchemaTypeIsStringOrBytes(Schema inputSchema, String fieldName, boolean isNullable) {
    Schema fieldSchema = inputSchema.getField(fieldName).getSchema();
    boolean fieldIsNullable = fieldSchema.isNullable();
    if (!isNullable && fieldIsNullable) {
      throw new IllegalArgumentException("Field " + fieldName + " cannot be nullable");
    }
    Schema.Type fieldType = fieldIsNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (fieldType != Schema.Type.STRING && fieldType != Schema.Type.BYTES) {
      throw new IllegalArgumentException(
        String.format("Field name %s is of type %s, only types String and Bytes are supported for KVTable",
                      fieldName, fieldType));
    }
  }

}
