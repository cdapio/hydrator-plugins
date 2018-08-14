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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;

/**
 * Action that moves file(s) within HDFS in the same cluster.
 * A user must specify file/directory path and destination file/directory path
 * Optionals include fileRegex
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("HDFSMove")
@Description("Action to move files within HDFS. (Deprecated. Use File Move instead.)")
@Deprecated
public class HDFSMoveAction extends Action {
  private FileMoveAction delegate;
  // only needed for plugin inspection to see it's properties
  @SuppressWarnings("FieldCanBeLocal")
  private final FileMoveAction.Conf config;

  public HDFSMoveAction(FileMoveAction.Conf config) {
    this.delegate = new FileMoveAction(config);
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    delegate.run(context);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    delegate.configurePipeline(pipelineConfigurer);
  }
}
