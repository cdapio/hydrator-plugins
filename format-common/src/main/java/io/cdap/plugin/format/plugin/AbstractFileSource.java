/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.plugin;

import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.RegexPathFilter;
import io.cdap.plugin.format.input.EmptyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Common logic for a source that reads from a Hadoop FileSystem. Supports functionality that is common across any
 * FileSystem, whether it be HDFS, GCS, S3, Azure, etc.
 *
 * {@link FileSourceProperties} contains the set of properties that this source understands.
 *
 * Plugins should extend this class and simply provide any additional Configuration properties that are required
 * by their specific FileSystem, such as credential information.
 * Their PluginConfig should implement FileSourceProperties and be passed into the constructor of this class.
 *
 * @param <T> type of config
 */
public abstract class AbstractFileSource<T extends PluginConfig & FileSourceProperties>
  extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final String FORMAT_PLUGIN_ID = "format";
  private final T config;

  protected AbstractFileSource(T config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);

    FileFormat fileFormat = config.getFormat();
    Schema schema = null;
    ValidatingInputFormat validatingInputFormat =
      pipelineConfigurer.usePlugin(ValidatingInputFormat.PLUGIN_TYPE, fileFormat.name().toLowerCase(), FORMAT_PLUGIN_ID,
                                   config.getProperties());
    FormatContext context = new FormatContext(collector, null);
    validateInputFormatProvider(context, fileFormat, validatingInputFormat);

    if (validatingInputFormat != null) {
      schema = validatingInputFormat.getSchema(context);
    }

    validatePathField(collector, schema);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    ValidatingInputFormat validatingInputFormat = context.newPluginInstance(FORMAT_PLUGIN_ID);
    FileFormat fileFormat = config.getFormat();
    FormatContext formatContext = new FormatContext(collector, context.getInputSchema());
    validateInputFormatProvider(formatContext, fileFormat, validatingInputFormat);
    validatePathField(collector, validatingInputFormat.getSchema(formatContext));
    collector.getOrThrowException();


    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Pattern pattern = config.getFilePattern();
    if (pattern != null) {
      RegexPathFilter.configure(conf, pattern);
      FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
    }
    FileInputFormat.setInputDirRecursive(job, config.shouldReadRecursively());

    Schema schema = config.getSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    if (schema != null && schema.getFields() != null) {
      recordLineage(lineageRecorder,
                    schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }

    // set entries here, before FileSystem is used
    for (Map.Entry<String, String> entry : getFileSystemProperties(context).entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    Path path = new Path(config.getPath());
    FileSystem pathFileSystem = FileSystem.get(path.toUri(), conf);

    FileStatus[] fileStatus = pathFileSystem.globStatus(path);

    String inputFormatClass;
    if (fileStatus == null) {
      if (config.shouldAllowEmptyInput()) {
        inputFormatClass = EmptyInputFormat.class.getName();
      } else {
        throw new IOException(String.format("Input path %s does not exist", path));
      }
    } else {
      FileInputFormat.addInputPath(job, path);
      FileInputFormat.setMaxInputSplitSize(job, config.getMaxSplitSize());
      inputFormatClass = validatingInputFormat.getInputFormatClassName();
      Configuration hConf = job.getConfiguration();
      Map<String, String> inputFormatConfiguration = validatingInputFormat.getInputFormatConfiguration();
      for (Map.Entry<String, String> propertyEntry : inputFormatConfiguration.entrySet()) {
        hConf.set(propertyEntry.getKey(), propertyEntry.getValue());
      }
    }

    // set entries here again, in case anything set by PathTrackingInputFormat should be overridden
    for (Map.Entry<String, String> entry : getFileSystemProperties(context).entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    context.setInput(Input.of(config.getReferenceName(), new SourceInputFormatProvider(inputFormatClass, conf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input.getValue());
  }

  /**
   * Override this to provide any additional Configuration properties that are required by the FileSystem.
   * For example, if the FileSystem requires setting properties for credentials, those should be returned by
   * this method.
   */
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    return Collections.emptyMap();
  }

  /**
   * Override this to specify a custom field level operation name and description.
   */
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", String.format("Read from %s files.",
                                                     config.getFormat().name().toLowerCase()), outputFields);
  }

  private void validateInputFormatProvider(FormatContext context, FileFormat fileFormat,
                                           @Nullable ValidatingInputFormat validatingInputFormat) {
    FailureCollector collector = context.getFailureCollector();
    if (validatingInputFormat == null) {
      collector.addFailure(
        String.format("Could not find the '%s' input format.", fileFormat.name().toLowerCase()), null)
        .withPluginNotFound(FORMAT_PLUGIN_ID, fileFormat.name().toLowerCase(), ValidatingInputFormat.PLUGIN_TYPE);
    } else {
      validatingInputFormat.validate(context);
    }
  }

  private void validatePathField(FailureCollector collector, Schema schema) {
    String pathField = config.getPathField();
    if (pathField != null && schema != null) {
      Schema.Field schemaPathField = schema.getField(pathField);
      if (schemaPathField == null) {
        collector.addFailure(String.format("Path field '%s' must exist in input schema.", pathField), null)
          .withConfigProperty(AbstractFileSourceConfig.PATH_FIELD);
        throw collector.getOrThrowException();
      }
      Schema pathFieldSchema = schemaPathField.getSchema();
      Schema nonNullableSchema = pathFieldSchema.isNullable() ? pathFieldSchema.getNonNullable() : pathFieldSchema;

      if (nonNullableSchema.getType() != Schema.Type.STRING) {
        collector.addFailure(String.format("Path field '%s' is of unsupported type '%s'.", pathField,
                                           nonNullableSchema.getDisplayName()),
                             "It must be of type 'string'.")
          .withConfigProperty(AbstractFileSourceConfig.PATH_FIELD)
          .withOutputSchemaField(schemaPathField.getName());
      }
    }
  }
}
