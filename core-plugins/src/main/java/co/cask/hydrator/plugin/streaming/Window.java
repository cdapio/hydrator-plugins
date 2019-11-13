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

package co.cask.hydrator.plugin.streaming;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.streaming.Windower;

/**
 * Window plugin.
 */
@Plugin(type = Windower.PLUGIN_TYPE)
@Name("Window")
@Description("Windows a pipeline.")
public class Window extends Windower {
  private final Conf conf;

  public Window(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public long getWidth() {
    return conf.width;
  }

  @Override
  public long getSlideInterval() {
    return conf.slideInterval;
  }

  /**
   * Config for window plugin.
   */
  public static class Conf extends PluginConfig {
    @Description("Width of the window in seconds. Must be a multiple of the pipeline's batch interval. " +
      "Each window generated will contain all the events from this many seconds.")
    long width;

    @Description("Sliding interval of the window in seconds. Must be a multiple of the pipeline's batch interval. " +
      "Every this number of seconds, a new window will be generated.")
    long slideInterval;
  }
}
