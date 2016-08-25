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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.common.batch.action.Condition;
import co.cask.hydrator.common.batch.action.ConditionConfig;
import co.cask.hydrator.plugin.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import javax.annotation.Nullable;

/**
 * Runs a query after a pipeline run.
 */
@SuppressWarnings("ConstantConditions")
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("DatabaseQuery")
@Description("Runs a query after a pipeline run.")
public class QueryAction extends PostAction {
  private static final String JDBC_PLUGIN_ID = "driver";
  private static final Logger LOG = LoggerFactory.getLogger(QueryAction.class);
  private final QueryActionConfig config;

  public QueryAction(QueryActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(BatchActionContext batchContext) throws Exception {
    config.validate();

    if (!config.shouldRun(batchContext)) {
      return;
    }

    Class<? extends Driver> driverClass = batchContext.loadPluginClass(JDBC_PLUGIN_ID);
    DBRun executeQuery = new DBRun(config, driverClass);
    executeQuery.run();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate();
    DBManager dbManager = new DBManager(config);
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, JDBC_PLUGIN_ID);
  }


  /**
   * config for {@link QueryAction}
   */
  public class QueryActionConfig extends QueryConfig {
    @Nullable
    @Description("When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'success'. " +
      "If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or " +
      "failed. If set to 'success', the action will only be executed if the pipeline run succeeded. " +
      "If set to 'failure', the action will only be executed if the pipeline run failed.")
    @Macro
    public String runCondition;

    public QueryActionConfig() {
      super();
      runCondition = Condition.SUCCESS.name();
    }

    public void validate() {
      // have to delegate instead of inherit, since we can't extend both ConditionConfig and ConnectionConfig.
      if (!containsMacro("runCondition")) {
        new ConditionConfig(runCondition).validate();
      }
    }

    public boolean shouldRun(BatchActionContext actionContext) {
      return new ConditionConfig(runCondition).shouldRun(actionContext);
    }
  }
}
