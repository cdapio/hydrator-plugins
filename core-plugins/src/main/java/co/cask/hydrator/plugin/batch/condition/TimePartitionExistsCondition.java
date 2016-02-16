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

package co.cask.hydrator.plugin.batch.condition;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.RunCondition;
import co.cask.hydrator.common.ETLTime;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Waits for a specific partition of a TimePartitionedFileSet exists.
 */
@Plugin(type = RunCondition.PLUGIN_TYPE)
@Name("TimePartitionExists")
@Description("Waits until a specific partition of a TimePartitionedFileSet exists.")
public class TimePartitionExistsCondition extends RunCondition {
  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionExistsCondition.class);
  private final Config config;

  public TimePartitionExistsCondition(Config config) {
    this.config = config;
  }

  /**
   * Config for the partition.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the TimePartitionedFileSet to check.")
    private String name;

    @Description("The partition for this time must exist for this check to pass. " +
      "This should either be a time in milliseconds, or the string 'runtime'. " +
      "If 'runtime' is given, the runtime for the pipeline run will be used.")
    private String partitionTime;

    @Nullable
    @Description("Optional offset to subtract from the partitionTime. " +
      "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
      "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. " +
      "For example, a value of '2h' means that two hours will be subtracted from the runtime before macro substitution")
    private String offset;

    @Nullable
    @Description("How often to check for existence of the partition. Defaults to 60 seconds.")
    private Long pollSeconds;

    public Config() {
      this.offset = "0m";
      this.pollSeconds = 60L;
    }

    private long getPartitionTime(long runtime) {
      long answer;
      if ("runtime".equals(partitionTime.toLowerCase())) {
        answer = runtime;
      } else {
        answer = Long.parseLong(partitionTime);
      }
      if (offset != null) {
        answer -= ETLTime.parseDuration(offset);
      }
      return answer;
    }
  }

  @Override
  public void waitUntilReady(BatchRuntimeContext batchRuntimeContext) throws Exception {
    TimePartitionedFileSet tpfs = batchRuntimeContext.getDataset(config.name);

    long partitionTime = config.getPartitionTime(batchRuntimeContext.getLogicalStartTime());

    LOG.info("Waiting for partition for time {}.", partitionTime);
    // first check if the fileset says the partition exists. If so, we can return now
    if (tpfs.getPartitionByTime(partitionTime) != null) {
      return;
    }

    TransactionAware txAware = (TransactionAware) tpfs;
    while (true) {
      TimeUnit.SECONDS.sleep(config.pollSeconds);

      // horrible hacks...
      // this runs in a long running transaction (mapreduce), which means we will never be able to see newly added
      // partitions. We could look at the location of the expected partition and check for existence of that location,
      // but this is hugely unreliable. For example, mapreduce may decide to create that directory way before
      // anything is ever written to it.
      // so... create a manual Transaction, which means we may see invalid and in-progress writes...
      Transaction transaction = new Transaction(Long.MAX_VALUE, Long.MAX_VALUE, new long[] {}, new long[] {}, 0L);
      txAware.startTx(transaction);

      if (tpfs.getPartitionByTime(partitionTime) != null) {
        return;
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    // Assumes the TimePartitionedFileSet already exists
    // try to get the partition time in order to catch any syntax errors, etc.
    config.getPartitionTime(System.currentTimeMillis());
  }
}
