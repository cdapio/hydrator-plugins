/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.common.LineageRecorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Clones Input Record 'n' number of times into output.
 */
@Plugin(type = "transform")
@Name("CloneRecord")
@Description("Clone input records 'n' number of times into output")
public final class CloneRecord extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  // Required only for testing.
  public CloneRecord(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null || inputSchema.getFields() == null || inputSchema.getFields().isEmpty()) {
      return;
    }
    Set<String> input = inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());

    List<FieldOperation> operationList = new ArrayList<>();
    for (String inputField : input) {
      List<String> outputs = new ArrayList<>();
      for (int i = 0; i < config.copies; i++) {
        outputs.add(inputField);
      }
      FieldTransformOperation operation =
          new FieldTransformOperation("clone" + inputField, "Clone field " + inputField,
                                      Collections.singletonList(inputField), outputs);
      operationList.add(operation);
    }
    context.record(operationList);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    List<Schema.Field> fields = in.getSchema().getFields();
    for (int i = 0; i < config.copies; ++i) {
      StructuredRecord.Builder builder = StructuredRecord.builder(in.getSchema());
      for (Schema.Field field : fields) {
        String name = field.getName();
        builder.set(name, in.get(name));
      }
      emitter.emit(builder.build());
    }
  }

  /**
   * Clone rows plugin configuration.
   */
  public static class Config extends PluginConfig {
    private static final String NAME_COPIES = "copies";

    @Name(NAME_COPIES)
    @Description("Specifies number of copies to be made of every record.")
    @Macro
    private final int copies;

    public Config(int copies) {
      this.copies = copies;
    }

    private void validate(FailureCollector collector) {
      if (!containsMacro("copies") && (copies <= 0)) {
        collector.addFailure("Number of copies must be a positive number.", null).withConfigProperty(NAME_COPIES);
      }
    }
  }

  private void recordLineage(BatchContext context, String outputName, Schema tableSchema, List<String> fieldNames) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to Database.", fieldNames);
    }
  }
}
