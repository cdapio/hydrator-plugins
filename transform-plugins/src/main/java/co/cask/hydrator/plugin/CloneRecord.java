/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

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
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    config.validate();
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
    @Name("copies")
    @Description("Specifies number of copies to be made of every record.")
    @Macro
    private final int copies;
    
    public Config(int copies) {
      this.copies = copies;
    }

    private void validate() {
      if (!containsMacro("copies") && (copies == 0 || copies > Integer.MAX_VALUE)) {
        throw new IllegalArgumentException("Number of copies specified '" + copies + "' is incorrect. Specify " +
                                             "proper integer range");
      }
    }
  }
}
