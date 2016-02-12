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

package co.cask.hydrator.plugin.db.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.hydrator.common.batch.action.Condition;
import co.cask.hydrator.common.batch.action.ConditionConfig;
import co.cask.hydrator.plugin.ConnectionConfig;

import javax.annotation.Nullable;

/**
 * Config for {@link QueryAction}.
 */
public class QueryConfig extends ConnectionConfig {

  @Description("The query to run.")
  public String query;

  @Nullable
  @Description("When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'success'. " +
    "If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed." +
    "If set to 'success', the action will only be executed if the pipeline run succeeded. " +
    "If set to 'failure', the action will only be executed if the pipeline run failed.")
  public String runCondition;

  public QueryConfig() {
    super();
    runCondition = Condition.SUCCESS.name();
  }

  public void validate() {
    // have to delegate instead of inherit, since we can't extend both ConditionConfig and ConnectionConfig.
    new ConditionConfig(runCondition).validate();
  }

  public boolean shouldRun(BatchActionContext actionContext) {
    return new ConditionConfig(runCondition).shouldRun(actionContext);
  }
}
