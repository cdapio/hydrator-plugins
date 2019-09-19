/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.error;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Adds the error code, message, and stage to each record, then emits it.
 */
@Plugin(type = ErrorTransform.PLUGIN_TYPE)
@Name("ErrorCollector")
public class ErrorCollector extends ErrorTransform<StructuredRecord, StructuredRecord> {
  private final Config config;

  public ErrorCollector(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      if (config.messageField != null && inputSchema.getField(config.messageField) != null) {
        collector.addFailure(
          String.format("Input schema already contains field '%s'.", config.messageField),
          "Set message field to a different value.")
          .withConfigProperty(Config.MESSAGE_FIELD).withInputSchemaField(config.messageField);
      }
      if (config.codeField != null && inputSchema.getField(config.codeField) != null) {
        collector.addFailure(
          String.format("Input schema already contains field '%s'.", config.codeField),
          "Set code field to a different value.")
          .withConfigProperty(Config.CODE_FIELD).withInputSchemaField(config.codeField);
      }
      if (config.stageField != null && inputSchema.getField(config.stageField) != null) {
        collector.addFailure(
          String.format("Input schema already contains field '%s'.", config.stageField),
          "Set stage field to a different value.")
          .withConfigProperty(Config.STAGE_FIELD).withInputSchemaField(config.stageField);
      }
      Schema outputSchema = getOutputSchema(config, inputSchema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }
  }

  @Override
  public void transform(ErrorRecord<StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord invalidRecord = input.getRecord();
    StructuredRecord.Builder output = StructuredRecord.builder(getOutputSchema(config, invalidRecord.getSchema()));
    for (Schema.Field field : invalidRecord.getSchema().getFields()) {
      output.set(field.getName(), invalidRecord.get(field.getName()));
    }
    if (config.messageField != null) {
      output.set(config.messageField, input.getErrorMessage());
    }
    if (config.codeField != null) {
      output.set(config.codeField, input.getErrorCode());
    }
    if (config.stageField != null) {
      output.set(config.stageField, input.getStageName());
    }
    emitter.emit(output.build());
  }

  private static Schema getOutputSchema(Config config, Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(inputSchema.getFields());
    if (config.messageField != null) {
      fields.add(Schema.Field.of(config.messageField, Schema.of(Schema.Type.STRING)));
    }
    if (config.codeField != null) {
      fields.add(Schema.Field.of(config.codeField, Schema.of(Schema.Type.INT)));
    }
    if (config.stageField != null) {
      fields.add(Schema.Field.of(config.stageField, Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf("error" + inputSchema.getRecordName(), fields);
  }

  /**
   * The plugin config
   */
  public static class Config extends PluginConfig {
    public static final String MESSAGE_FIELD = "messageField";
    public static final String CODE_FIELD = "codeField";
    public static final String STAGE_FIELD = "stageField";

    @Nullable
    @Description("The name of the error message field to use in the output schema. " +
      "If this not specified, the error message will be dropped.")
    private String messageField;

    @Nullable
    @Description("The name of the error code field to use in the output schema. " +
      "If this not specified, the error code will be dropped.")
    private String codeField;

    @Nullable
    @Description("The name of the error stage field to use in the output schema. " +
      "If this not specified, the error stage will be dropped.")
    private String stageField;

  }
}
