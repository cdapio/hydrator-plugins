/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.sink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.TimeParser;
import io.cdap.plugin.dataset.SnapshotFileSet;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Sink that stores snapshots on HDFS, and keeps track of which snapshot is the latest snapshot.
 *
 * @param <T> the type of plugin config
 */
public abstract class SnapshotFileBatchSink<T extends SnapshotFileSetBatchSinkConfig>
  extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotFileBatchSink.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String FORMAT_PLUGIN_ID = "format";

  private final T config;
  private SnapshotFileSet snapshotFileSet;

  public SnapshotFileBatchSink(T config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    String outputFormatName = getOutputFormatPlugin();
    OutputFormatProvider outputFormatProvider =
      pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, outputFormatName, FORMAT_PLUGIN_ID,
                                   config.getProperties());
    if (outputFormatProvider == null) {
      throw new IllegalArgumentException(
        String.format("Could not find the '%s' output format plugin. "
                        + "Please ensure the '%s' format plugin is installed.", outputFormatName, outputFormatName));
    }
    // validate config properties
    outputFormatProvider.getOutputFormatConfiguration();

    if (!config.containsMacro("name") && !config.containsMacro("basePath") && !config.containsMacro("fileProperties")) {
      FileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
      addFileProperties(fileProperties);
      pipelineConfigurer.createDataset(config.getName(), PartitionedFileSet.class,
                                       createProperties(outputFormatProvider));
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException, InstantiationException {
    // if macros were provided, the dataset still needs to be created
    config.validate();
    OutputFormatProvider outputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);
    DatasetProperties datasetProperties = createProperties(outputFormatProvider);
    if (!context.datasetExists(config.getName())) {
      context.createDataset(config.getName(), PartitionedFileSet.class.getName(), datasetProperties);
    }

    PartitionedFileSet files = context.getDataset(config.getName());
    snapshotFileSet = new SnapshotFileSet(files);

    // need to use all the dataset properties as arguments in case the dataset already exists,
    // created by the previous version of this plugin before output format plugins were used.
    // in that scenario, the output format attached to the dataset properties will be incorrect,
    // and must be overridden here.
    Map<String, String> arguments = new HashMap<>(datasetProperties.getProperties());

    if (config.getFileProperties() != null) {
      arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
    }
    context.addOutput(Output.ofDataset(config.getName(),
                                       snapshotFileSet.getOutputArguments(context.getLogicalStartTime(), arguments)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
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

  private DatasetProperties createProperties(OutputFormatProvider outputFormatProvider) {
    FileSetProperties.Builder fileProperties = SnapshotFileSet.getBaseProperties(config);
    addFileProperties(fileProperties);

    fileProperties.setOutputFormat(outputFormatProvider.getOutputFormatClassName());
    for (Map.Entry<String, String> formatProperty : outputFormatProvider.getOutputFormatConfiguration().entrySet()) {
      fileProperties.setOutputProperty(formatProperty.getKey(), formatProperty.getValue());
    }
    return fileProperties.build();
  }

  protected abstract String getOutputFormatPlugin();

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder);

}
