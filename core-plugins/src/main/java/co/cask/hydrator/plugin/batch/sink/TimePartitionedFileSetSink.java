/*
 * Copyright © 2015, 2016 Cask Data, Inc.
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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import co.cask.hydrator.common.TimeParser;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
    // create the dataset at configure time if no macros were provided on necessary fields
    if (!tpfsSinkConfig.containsMacro("name") && !tpfsSinkConfig.containsMacro("basePath") &&
      !tpfsSinkConfig.containsMacro("schema")) {
      String tpfsName = tpfsSinkConfig.name;
      FileSetProperties.Builder properties = FileSetProperties.builder();
      if (!Strings.isNullOrEmpty(tpfsSinkConfig.basePath)) {
        properties.setBasePath(tpfsSinkConfig.basePath);
      }
      addFileSetProperties(properties);
      pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), properties.build());
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    tpfsSinkConfig.validate();
    // if macros were provided and the dataset doesn't exist, create it now
    if (!context.datasetExists(tpfsSinkConfig.name)) {
      String tpfsName = tpfsSinkConfig.name;
      FileSetProperties.Builder properties = FileSetProperties.builder();

      if (!Strings.isNullOrEmpty(tpfsSinkConfig.basePath)) {
        properties.setBasePath(tpfsSinkConfig.basePath);
      }
      addFileSetProperties(properties);
      context.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), properties.build());
    }
    long outputPartitionTime = context.getLogicalStartTime();
    if (tpfsSinkConfig.partitionOffset != null) {
      outputPartitionTime -= TimeParser.parseDuration(tpfsSinkConfig.partitionOffset);
    }
    Map<String, String> sinkArgs = getAdditionalTPFSArguments();
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

  /**
   * @return any additional properties that need to be set for the sink. For example, avro sink requires
   *         setting some schema output key.
   */
  protected Map<String, String> getAdditionalTPFSArguments() {
    // release 1.4 hydrator plugins uses FileSetUtil to set all the properties that the input and output formats
    // require when it creates the dataset, so it doesn't need to set those arguments at runtime. inorder to be
    // backward compatible to older versions of the plugins we need to set this at runtime.
    return new HashMap<>();
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
