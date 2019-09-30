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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.TimeParser;
import org.apache.hadoop.io.NullWritable;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class to read from a {@link TimePartitionedFileSet}.
 *
 * @param <T> type of plugin config
 */
public abstract class TimePartitionedFileSetSource<T extends TPFSConfig>
  extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final String FORMAT_PLUGIN_ID = "format";
  protected final T config;

  public TimePartitionedFileSetSource(T config) {
    this.config = config;
  }

  protected abstract String getInputFormatName();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();

    String inputFormatName = getInputFormatName();
    InputFormatProvider inputFormatProvider =
      pipelineConfigurer.usePlugin(ValidatingInputFormat.PLUGIN_TYPE, inputFormatName, FORMAT_PLUGIN_ID,
                                   config.getProperties());
    if (inputFormatProvider == null) {
      throw new IllegalArgumentException(
        String.format("Could not find the '%s' input format plugin. "
                        + "Please ensure the '%s' format plugin is installed.", inputFormatName, inputFormatName));
    }
    // get input format configuration to give the output format plugin a chance to validate it's config
    // and fail pipeline deployment if it is invalid
    inputFormatProvider.getInputFormatConfiguration();

    if (!config.containsMacro("name") && !config.containsMacro("basePath")) {
      String tpfsName = config.getName();
      pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(),
                                       createProperties(inputFormatProvider));
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws DatasetManagementException, InstantiationException {
    config.validate();

    InputFormatProvider inputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);
    DatasetProperties datasetProperties = createProperties(inputFormatProvider);

    // If macros provided at runtime, dataset still needs to be created
    if (!context.datasetExists(config.getName())) {
      String tpfsName = config.getName();
      context.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), datasetProperties);
    }

    Schema schema = config.getSchema();
    if (schema.getFields() != null) {
      String formatName = getInputFormatName();
      FieldOperation operation =
        new FieldReadOperation("Read", String.format("Read from TimePartitionedFileSet in %s format.", formatName),
                               EndPoint.of(context.getNamespace(), config.getName()),
                               schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }

    long duration = TimeParser.parseDuration(config.getDuration());
    long delay = Strings.isNullOrEmpty(config.getDelay()) ? 0 : TimeParser.parseDuration(config.getDelay());
    long endTime = context.getLogicalStartTime() - delay;
    long startTime = endTime - duration;
    Map<String, String> sourceArgs = Maps.newHashMap(datasetProperties.getProperties());
    TimePartitionedFileSetArguments.setInputStartTime(sourceArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(sourceArgs, endTime);
    context.setInput(Input.ofDataset(config.getName(), sourceArgs));
  }

  private DatasetProperties createProperties(InputFormatProvider inputFormatProvider) {
    FileSetProperties.Builder properties = FileSetProperties.builder();

    if (!Strings.isNullOrEmpty(config.getBasePath())) {
      properties.setBasePath(config.getBasePath());
    }

    properties.setInputFormat(inputFormatProvider.getInputFormatClassName());
    for (Map.Entry<String, String> formatProperty : inputFormatProvider.getInputFormatConfiguration().entrySet()) {
      properties.setInputProperty(formatProperty.getKey(), formatProperty.getValue());
    }
    addFileSetProperties(properties);
    return properties.build();
  }

  /**
   * Set file set specific properties, such as input/output format and explore properties.
   */
  protected abstract void addFileSetProperties(FileSetProperties.Builder properties);
}
