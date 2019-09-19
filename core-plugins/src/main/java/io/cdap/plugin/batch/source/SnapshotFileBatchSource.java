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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.batch.sink.SnapshotFileBatchSink;
import io.cdap.plugin.dataset.SnapshotFileSet;
import org.apache.hadoop.io.NullWritable;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads the latest snapshot written by a {@link SnapshotFileBatchSink}.
 *
 * @param <T> type of plugin config
 */
public abstract class SnapshotFileBatchSource<T extends SnapshotFileSetSourceConfig>
  extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String FORMAT_PLUGIN_ID = "format";

  protected final T config;

  public SnapshotFileBatchSource(T config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
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

    if (!config.containsMacro("name") && !config.containsMacro("basePath") && !config.containsMacro("fileProperties")) {
      pipelineConfigurer.createDataset(config.getName(), PartitionedFileSet.class,
                                       createProperties(inputFormatProvider));
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    InputFormatProvider inputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);
    DatasetProperties datasetProperties = createProperties(inputFormatProvider);

    // Dataset must still be created if macros provided at configure time
    if (!context.datasetExists(config.getName())) {
      context.createDataset(config.getName(), PartitionedFileSet.class.getName(), datasetProperties);
    }

    PartitionedFileSet partitionedFileSet = context.getDataset(config.getName());
    SnapshotFileSet snapshotFileSet = new SnapshotFileSet(partitionedFileSet);

    Map<String, String> arguments = new HashMap<>(datasetProperties.getProperties());

    if (config.getFileProperties() != null) {
      arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
    }

    Schema schema = config.getSchema();
    if (schema.getFields() != null) {
      String formatName = getInputFormatName();
      FieldOperation operation =
        new FieldReadOperation("Read", String.format("Read from SnapshotFile source in %s format.", formatName),
                               EndPoint.of(context.getNamespace(), config.getName()),
                               schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }

    context.setInput(Input.ofDataset(config.getName(), snapshotFileSet.getInputArguments(arguments)));
  }

  private DatasetProperties createProperties(InputFormatProvider inputFormatProvider) {
    FileSetProperties.Builder properties = SnapshotFileSet.getBaseProperties(config);

    if (!Strings.isNullOrEmpty(config.getBasePath())) {
      properties.setBasePath(config.getBasePath());
    }

    properties.setInputFormat(inputFormatProvider.getInputFormatClassName());
    for (Map.Entry<String, String> formatProperty : inputFormatProvider.getInputFormatConfiguration().entrySet()) {
      properties.setInputProperty(formatProperty.getKey(), formatProperty.getValue());
    }
    addFileProperties(properties);
    return properties.build();
  }

  protected abstract String getInputFormatName();

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder);

}
