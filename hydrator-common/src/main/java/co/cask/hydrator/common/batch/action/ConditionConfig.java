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

package co.cask.hydrator.common.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import com.google.common.base.Joiner;

import javax.annotation.Nullable;

/**
 * Base plugin config for post actions that contain a setting for when the action should run.
 */
public class ConditionConfig extends PluginConfig {

  @Nullable
  @Description("When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'completion'. " +
    "If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed." +
    "If set to 'success', the action will only be executed if the pipeline run succeeded. " +
    "If set to 'failure', the action will only be executed if the pipeline run failed.")
  public String runCondition;

  public ConditionConfig() {
    this.runCondition = Condition.COMPLETION.name();
  }

  public ConditionConfig(String runCondition) {
    this.runCondition = runCondition;
  }

  @SuppressWarnings("ConstantConditions")
  public void validate() {
    try {
      Condition.valueOf(runCondition.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
        "Invalid runCondition value '%s'.  Must be one of %s.",
        runCondition, Joiner.on(',').join(Condition.values())));
    }
  }

  @SuppressWarnings("ConstantConditions")
  public boolean shouldRun(BatchActionContext actionContext) {
    Condition condition = Condition.valueOf(runCondition.toUpperCase());
    switch (condition) {
      case COMPLETION:
        return true;
      case SUCCESS:
        return actionContext.isSuccessful();
      case FAILURE:
        return !actionContext.isSuccessful();
      default:
        throw new IllegalStateException("Unknown value for runCondition: " + runCondition);
    }
  }
}
