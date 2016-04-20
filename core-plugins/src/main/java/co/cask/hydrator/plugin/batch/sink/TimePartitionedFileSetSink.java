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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.TimeParser;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TPFS Batch Sink class that stores sink data
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class TimePartitionedFileSetSink<KEY_OUT, VAL_OUT>
  extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedFileSetSink.class);

  protected final TPFSSinkConfig tpfsSinkConfig;

  protected TimePartitionedFileSetSink(TPFSSinkConfig tpfsSinkConfig) {
    this.tpfsSinkConfig = tpfsSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    tpfsSinkConfig.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = getAdditionalTPFSArguments();
    long outputPartitionTime = context.getLogicalStartTime();
    if (tpfsSinkConfig.partitionOffset != null) {
      outputPartitionTime -= TimeParser.parseDuration(tpfsSinkConfig.partitionOffset);
    }
    LOG.info("Writing to output partition of time {}.", outputPartitionTime);
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, outputPartitionTime);
    if (!Strings.isNullOrEmpty(tpfsSinkConfig.filePathFormat)) {
      TimePartitionedFileSetArguments.setOutputPathFormat(sinkArgs, tpfsSinkConfig.filePathFormat,
                                                          tpfsSinkConfig.timeZone);
    }
    context.addOutput(tpfsSinkConfig.name, sinkArgs);
  }

  /**
   * @return any additional properties that need to be set for the sink. For example, avro sink requires
   *         setting some schema output key.
   */
  protected Map<String, String> getAdditionalTPFSArguments() {
    return new HashMap<>();
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (succeeded && tpfsSinkConfig.cleanPartitionsOlderThan != null) {
      long cutoffTime =
        context.getLogicalStartTime() - TimeParser.parseDuration(tpfsSinkConfig.cleanPartitionsOlderThan);
      TimePartitionedFileSet tpfs = context.getDataset(tpfsSinkConfig.name);
      for (TimePartitionDetail timePartitionDetail : tpfs.getPartitionsByTime(0, cutoffTime)) {
        LOG.info("Cleaning up old partition for timestamp {}", timePartitionDetail.getTime());
        tpfs.dropPartition(timePartitionDetail.getTime());
      }
    }
  }
}
