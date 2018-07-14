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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.TimeParser;
import co.cask.hydrator.plugin.common.SnapshotFileSetConfig;
import co.cask.hydrator.plugin.dataset.SnapshotFileSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink that stores snapshots on HDFS, and keeps track of which snapshot is the latest snapshot.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class SnapshotFileBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotFileBatchSink.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final SnapshotFileSetBatchSinkConfig config;
  private SnapshotFileSet snapshotFileSet;

  public SnapshotFileBatchSink(SnapshotFileSetBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (!config.containsMacro("name") && !config.containsMacro("basePath") && !config.containsMacro("fileProperties")) {
      FileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
      addFileProperties(fileProperties);
      pipelineConfigurer.createDataset(config.getName(), PartitionedFileSet.class, fileProperties.build());
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    // if macros were provided, the dataset still needs to be created
    config.validate();
    if (!context.datasetExists(config.getName())) {
      FileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
      addFileProperties(fileProperties);
      context.createDataset(config.getName(), PartitionedFileSet.class.getName(), fileProperties.build());
    }

    PartitionedFileSet files = context.getDataset(config.getName());
    snapshotFileSet = new SnapshotFileSet(files);

    Map<String, String> arguments = new HashMap<>();

    if (config.getFileProperties() != null) {
      arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
    }
    context.addOutput(Output.ofDataset(config.getName(),
                                       snapshotFileSet.getOutputArguments(context.getLogicalStartTime(), arguments)));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    super.onRunFinish(succeeded, context);
    if (succeeded) {
      try {
        snapshotFileSet.onSuccess(context.getLogicalStartTime());
      } catch (Exception e) {
        LOG.error("Exception updating state file with value of latest snapshot, ", e);
      }

      try {
        if (config.getCleanPartitionsOlderThan() != null) {
          long cutoffTime =
            context.getLogicalStartTime() - TimeParser.parseDuration(config.getCleanPartitionsOlderThan());
          snapshotFileSet.deleteMatchingPartitionsByTime(cutoffTime);
          LOG.debug("Cleaned up snapshots older than {}", config.getCleanPartitionsOlderThan());
        }
      } catch (IOException e) {
        LOG.error("Exception occurred while cleaning up older snapshots", e);
      }
    }
  }

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder);

  /**
   * Config for SnapshotFileBatchSink
   */
  public static class SnapshotFileSetBatchSinkConfig extends SnapshotFileSetConfig {
    @Description("Optional property that configures the sink to delete old partitions after successful runs. " +
      "If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and " +
      "delete any partitions older than that time. " +
      "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
      "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to " +
      "run at midnight of January 1, 2016, and this property is set to 7d, the sink will delete any partitions " +
      "for time partitions older than midnight Dec 25, 2015.")
    @Nullable
    @Macro
    protected String cleanPartitionsOlderThan;

    public SnapshotFileSetBatchSinkConfig() {

    }

    public SnapshotFileSetBatchSinkConfig(String name, @Nullable String basePath,
                                          @Nullable String cleanPartitionsOlderThan) {
      super(name, basePath, null);
      this.cleanPartitionsOlderThan = cleanPartitionsOlderThan;
    }

    public String getCleanPartitionsOlderThan() {
      return cleanPartitionsOlderThan;
    }

    public void validate() {
      if (cleanPartitionsOlderThan != null) {
        TimeParser.parseDuration(cleanPartitionsOlderThan);
      }
    }
  }
}
