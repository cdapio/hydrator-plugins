/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.plugin;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.RegexPathFilter;
import co.cask.hydrator.format.input.EmptyInputFormat;
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
    config.validate();

    Schema schema = config.getSchema();
    FileFormat fileFormat = config.getFormat();
    if (fileFormat != null) {
      InputFormatProvider inputFormatProvider =
        pipelineConfigurer.usePlugin("inputformat", fileFormat.name().toLowerCase(), FORMAT_PLUGIN_ID,
                                     config.getRawProperties());
      if (inputFormatProvider == null) {
        throw new IllegalArgumentException(String.format("Could not find the '%s' input format.",
                                                         fileFormat.name().toLowerCase()));
      }
    }

    String pathField = config.getPathField();
    if (pathField != null && schema != null) {
      Schema.Field schemaPathField = schema.getField(pathField);
      if (schemaPathField == null) {
        throw new IllegalArgumentException(
          String.format("Path field '%s' is not present in the schema. Please add it to the schema as a string field.",
                        pathField));
      }
      Schema pathFieldSchema = schemaPathField.getSchema();
      Schema.Type pathFieldType = pathFieldSchema.isNullable() ? pathFieldSchema.getNonNullable().getType() :
        pathFieldSchema.getType();
      if (pathFieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Path field '%s' must be of type 'string', but found '%s'.", pathField, pathFieldType));
      }
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();

    InputFormatProvider inputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);

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
      inputFormatClass = inputFormatProvider.getInputFormatClassName();
      Configuration hConf = job.getConfiguration();
      for (Map.Entry<String, String> propertyEntry : inputFormatProvider.getInputFormatConfiguration().entrySet()) {
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
}
