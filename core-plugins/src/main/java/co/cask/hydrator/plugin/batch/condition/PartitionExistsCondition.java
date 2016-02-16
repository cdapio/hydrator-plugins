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
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.RunCondition;
import co.cask.hydrator.common.MacroConfig;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Waits for a specific partition of a PartitionedFileSet exists.
 */
@Plugin(type = RunCondition.PLUGIN_TYPE)
@Name("PartitionExists")
@Description("Waits until a specific partition of a PartitionedFileSet exists.")
public class PartitionExistsCondition extends RunCondition {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionExistsCondition.class);
  private final Config config;

  public PartitionExistsCondition(Config config) {
    this.config = config;
  }

  /**
   * Config for the condition.
   */
  public static class Config extends MacroConfig {

    @Description("The name of the PartitionedFileSet to check.")
    private String name;

    @Description("Comma separated list of tuples that describe the partition. " +
      "Each tuple is a colon separated triplet of partition field name, value, and type. " +
      "Types are 'int', 'long', and 'string'." +
      "For example, 'shard:5:int,year:2016:int' will check that a partition key for shard=5 and year=2016 exists. " +
      "Also supports runtime macros which will be replaced with a value based on the runtime of the pipeline run.")
    private String partition;

    @Nullable
    @Description("How often to check for existence of the partition. Defaults to 60 seconds.")
    private Long pollSeconds;

    public Config() {
      this.pollSeconds = 60L;
    }

    private PartitionFilter getPartitionFilter() {
      PartitionFilter.Builder builder = PartitionFilter.builder();

      Splitter tupleSplitter = Splitter.on(',').trimResults();
      for (String tuple : tupleSplitter.split(partition)) {
        int colon1 = tuple.indexOf(':');
        if (colon1 < 0 || colon1 == (tuple.length() - 1)) {
          throw new IllegalArgumentException(
            String.format("Illegal partition tuple %s. " +
                            "Must contain colon separated partition field name, value and type. " +
                            "For example: 'year:2016:int,category:finance:string'", tuple));
        }
        String field = tuple.substring(0, colon1);

        int colon2 = tuple.indexOf(':', colon1 + 1);
        if (colon2 < 0) {
          throw new IllegalArgumentException(
            String.format("Illegal partition tuple %s. " +
                            "Must contain colon separated partition field name, value and type. " +
                            "For example: 'year:2016:int,category:finance:string'", tuple));
        }
        String value = tuple.substring(colon1 + 1, colon2);
        String type = tuple.substring(colon2 + 1).toLowerCase();
        if ("int".equals(type)) {
          builder.addValueCondition(field, Integer.parseInt(value));
        } else if ("string".equals(type)) {
          builder.addValueCondition(field, value);
        } else if ("long".equals(type)) {
          builder.addValueCondition(field, Long.parseLong(value));
        } else {
          throw new IllegalArgumentException(String.format("Invalid type %s in tuple %s.", type, tuple));
        }
      }
      return builder.build();
    }
  }

  @Override
  public void waitUntilReady(BatchRuntimeContext batchRuntimeContext) throws Exception {
    config.substituteMacros(batchRuntimeContext.getLogicalStartTime());
    PartitionedFileSet partitionedFileSet = batchRuntimeContext.getDataset(config.name);

    PartitionFilter partitionFilter = config.getPartitionFilter();
    LOG.debug("waiting for partitions");
    // first check if the fileset says the partition exists. If so, we can return now
    if (!partitionedFileSet.getPartitions(partitionFilter).isEmpty()) {
      return;
    }

    TransactionAware txAware = (TransactionAware) partitionedFileSet;
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

      if (!partitionedFileSet.getPartitions(partitionFilter).isEmpty()) {
        return;
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    // Assumes the PartitionedFileSet already exists
    // try to parse the partition in order to catch any syntax errors, etc.
    config.substituteMacros(0);
    config.getPartitionFilter();
  }
}
