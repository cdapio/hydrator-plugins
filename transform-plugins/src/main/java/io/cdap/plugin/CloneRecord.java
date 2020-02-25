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
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.util.List;

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

    // Return list of operations for 1-1 for every input field.
    context.record(
      TransformLineageRecorderUtils.generateOneToOnes(TransformLineageRecorderUtils.getFields(context.getInputSchema()),
                                               "clone",
                                               "Copied the input record."));
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
}
