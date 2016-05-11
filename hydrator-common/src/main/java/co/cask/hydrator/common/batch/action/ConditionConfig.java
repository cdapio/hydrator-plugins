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
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.hydrator.common.TimeParser;
import co.cask.hydrator.common.macro.MacroConfig;
import com.google.common.base.Joiner;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Base plugin config for post actions that contain a setting for when the action should run.
 */
public class ConditionConfig extends MacroConfig {
  private static final Pattern PARTITION_EXISTS_PATTERN = Pattern.compile("partitionHasData\\((.+)\\)");

  @Nullable
  @Description("When to run the action. Must be 'completion', 'success', 'failure', " +
    "or partitionHasData(dataset[,offset]). Defaults to 'completion'. " +
    "If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed. " +
    "If set to 'success', the action will only be executed if the pipeline run succeeded. " +
    "If set to 'failure', the action will only be executed if the pipeline run failed. " +
    "If set to 'partitionHasData(dataset[,offset])', the action will only be executed if a time partition " +
    "for the specified TimePartitionedFileSet exists and has some data in it. " +
    "The partition that is checked is the partition for the logical start time of the run minus the " +
    "specified offset. For example, 'partitionHasData(errors,1h)' will check for the existence of the partition of " +
    "the 'errors' TimePartitionedFileSet for one hour before the logical start time.")
  public String runCondition;

  public ConditionConfig() {
    this.runCondition = Condition.COMPLETION.name();
  }

  public ConditionConfig(String runCondition) {
    this.runCondition = runCondition;
  }

  @SuppressWarnings("ConstantConditions")
  public void validate() {
    if (parsePartitionExistsCondition() != null) {
      return;
    }

    try {
      Condition.valueOf(runCondition.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
        "Invalid runCondition value '%s'.  Must be one of %s, or partitionExists(<dataset name>).",
        runCondition, Joiner.on(',').join(Condition.values())));
    }
  }

  @SuppressWarnings("ConstantConditions")
  public boolean shouldRun(BatchActionContext actionContext) throws IOException {
    PartitionExistsCondition partitionExistsCondition = parsePartitionExistsCondition();
    if (partitionExistsCondition != null) {
      long partitionTime = actionContext.getLogicalStartTime() - partitionExistsCondition.offsetMillis;
      TimePartitionedFileSet tpfs = actionContext.getDataset(partitionExistsCondition.datasetName);
      TimePartitionDetail partitionDetail = tpfs.getPartitionByTime(partitionTime);
      if (partitionDetail == null) {
        return false;
      }
      for (Location file : partitionDetail.getLocation().list()) {
        if (!file.getName().startsWith(".") && file.length() > 0) {
          return true;
        }
      }
      return false;
    }

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

  @Nullable
  private PartitionExistsCondition parsePartitionExistsCondition() {
    // partitionHasData(datasetname) or partitionHasData(datasetname,offset)
    if (runCondition == null) {
      return null;
    }

    Matcher matcher = PARTITION_EXISTS_PATTERN.matcher(runCondition);
    if (matcher.matches()) {
      String arguments = matcher.group(1).trim();
      int commaIndex = arguments.indexOf(',');
      if (commaIndex == arguments.length() - 1) {
        throw new IllegalArgumentException(String.format(
          "Invalid run condition '%s'. " +
            "Arguments to partitionHasData are expected to be <dataset name>[,offset]. " +
            "For example 'partitionHasData(errors)' or 'partitionHasData(errors,1h)'.",
          arguments));
      }
      String datasetName = arguments;
      long offset = 0;
      if (commaIndex > 0) {
        datasetName = arguments.substring(0, commaIndex).trim();
        offset = TimeParser.parseDuration(arguments.substring(commaIndex + 1).trim());
      }
      return new PartitionExistsCondition(datasetName, offset);
    }
    return null;
  }

  private static class PartitionExistsCondition {
    private final String datasetName;
    private final long offsetMillis;

    public PartitionExistsCondition(String datasetName, long offsetMillis) {
      this.datasetName = datasetName;
      this.offsetMillis = offsetMillis;
    }
  }
}
