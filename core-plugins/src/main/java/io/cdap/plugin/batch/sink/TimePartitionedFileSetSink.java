/*
 * Copyright © 2015, 2016-2019 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.TimePartitionDetail;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.TimeParser;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TPFS Batch Sink class that stores sink data
 *
 * @param <T> the type of plugin config
 */
public abstract class TimePartitionedFileSetSink<T extends TPFSSinkConfig>
  extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedFileSetSink.class);
  private static final String FORMAT_PLUGIN_ID = "format";

  protected final T tpfsSinkConfig;

  protected TimePartitionedFileSetSink(T tpfsSinkConfig) {
    this.tpfsSinkConfig = tpfsSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    tpfsSinkConfig.validate();
    String outputFormatName = getOutputFormatName();
    OutputFormatProvider outputFormatProvider =
      pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, outputFormatName, FORMAT_PLUGIN_ID,
                                   tpfsSinkConfig.getProperties());
    if (outputFormatProvider == null) {
      throw new IllegalArgumentException(
        String.format("Could not find the '%s' output format plugin. "
                        + "Please ensure the '%s' format plugin is installed.", outputFormatName, outputFormatName));
    }
    // get output format configuration to give the output format plugin a chance to validate it's config
    // and fail pipeline deployment if it is invalid
    outputFormatProvider.getOutputFormatConfiguration();

    // create the dataset at configure time if no macros were provided on necessary fields
    if (!tpfsSinkConfig.containsMacro("name") && !tpfsSinkConfig.containsMacro("basePath") &&
      !tpfsSinkConfig.containsMacro("schema")) {
      pipelineConfigurer.createDataset(tpfsSinkConfig.name, TimePartitionedFileSet.class.getName(),
                                       createProperties(outputFormatProvider));
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException, InstantiationException {
    tpfsSinkConfig.validate();
    OutputFormatProvider outputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);
    DatasetProperties datasetProperties = createProperties(outputFormatProvider);
    // if macros were provided and the dataset doesn't exist, create it now
    if (!context.datasetExists(tpfsSinkConfig.name)) {
      context.createDataset(tpfsSinkConfig.name, TimePartitionedFileSet.class.getName(), datasetProperties);
    }

    // setup output path arguments
    long outputPartitionTime = context.getLogicalStartTime();
    if (tpfsSinkConfig.partitionOffset != null) {
      outputPartitionTime -= TimeParser.parseDuration(tpfsSinkConfig.partitionOffset);
    }

    // need to use all the dataset properties as arguments in case the dataset already exists,
    // created by the previous version of this plugin before output format plugins were used.
    // in that scenario, the output format attached to the dataset properties will be incorrect,
    // and must be overridden here.
    Map<String, String> sinkArgs = new HashMap<>(datasetProperties.getProperties());
    LOG.debug("Writing to output partition of time {}.", outputPartitionTime);
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, outputPartitionTime);
    if (!Strings.isNullOrEmpty(tpfsSinkConfig.filePathFormat)) {
      TimePartitionedFileSetArguments.setOutputPathFormat(sinkArgs, tpfsSinkConfig.filePathFormat,
                                                          tpfsSinkConfig.timeZone);
    }
    context.addOutput(Output.ofDataset(tpfsSinkConfig.name, sinkArgs));

    if (tpfsSinkConfig.schema != null) {
      try {
        Schema schema = Schema.parseJson(tpfsSinkConfig.schema);
        if (schema.getFields() != null) {
          FieldOperation operation =
            new FieldWriteOperation("Write", "Wrote to TPFS dataset",
                                    EndPoint.of(context.getNamespace(), tpfsSinkConfig.name),
                                    schema.getFields().stream().map(Schema.Field::getName)
                                      .collect(Collectors.toList()));
          context.record(Collections.singletonList(operation));
        }
      } catch (IOException e) {
        throw new IllegalStateException("Failed to parse schema.", e);
      }
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  protected abstract String getOutputFormatName();

  private DatasetProperties createProperties(OutputFormatProvider outputFormatProvider) {
    FileSetProperties.Builder properties = FileSetProperties.builder();

    if (!Strings.isNullOrEmpty(tpfsSinkConfig.basePath)) {
      properties.setBasePath(tpfsSinkConfig.basePath);
    }

    properties.setOutputFormat(outputFormatProvider.getOutputFormatClassName());
    for (Map.Entry<String, String> formatProperty : outputFormatProvider.getOutputFormatConfiguration().entrySet()) {
      properties.setOutputProperty(formatProperty.getKey(), formatProperty.getValue());
    }
    addFileSetProperties(properties);
    return properties.build();
  }

  /**
   * Set file set specific properties, such as input/output format and explore properties.
   */
  protected abstract void addFileSetProperties(FileSetProperties.Builder properties);

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (succeeded && tpfsSinkConfig.cleanPartitionsOlderThan != null) {
      long cutoffTime =
        context.getLogicalStartTime() - TimeParser.parseDuration(tpfsSinkConfig.cleanPartitionsOlderThan);
      TimePartitionedFileSet tpfs = context.getDataset(tpfsSinkConfig.name);
      for (TimePartitionDetail timePartitionDetail : tpfs.getPartitionsByTime(0, cutoffTime)) {
        LOG.info("Cleaning up partitions older than {}", tpfsSinkConfig.cleanPartitionsOlderThan);
        tpfs.dropPartition(timePartitionDetail.getTime());
      }
    }
  }
}
