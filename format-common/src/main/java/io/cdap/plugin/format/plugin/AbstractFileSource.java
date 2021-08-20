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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileSource.class);
  private static final String NAME_FORMAT = "format";
  private static final String FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
  private final T config;
  private static final Gson GSON = new Gson();

  protected AbstractFileSource(T config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    // throw exception if there were any errors while validating the config. This could happen if format or schema is
    // invalid
    collector.getOrThrowException();

    if (config.containsMacro(NAME_FORMAT)) {
      // Deploy all format plugins. This ensures that the required plugin is available when
      // the format macro is evaluated in prepareRun.
      for (FileFormat f : FileFormat.values()) {
        try {
          pipelineConfigurer.usePlugin(ValidatingInputFormat.PLUGIN_TYPE, f.name().toLowerCase(),
                                       f.name().toLowerCase(), config.getRawProperties());
        } catch (InvalidPluginConfigException e) {
          LOG.warn("Failed to register format '{}', which means it cannot be used when the pipeline is run. " +
                     "Missing properties: {}, invalid properties: {}", f.name(), e.getMissingProperties(),
                   e.getInvalidProperties().stream().map(InvalidPluginProperty::getName).collect(Collectors.toList()));
        }
      }
      return;
    }

    String fileFormat = config.getFormatName();

    // here set the schema from the config since the input format can only provide schema if all the required fields
    // are not macro
    Schema schema = config.getSchema();

    PluginProperties.Builder builder = PluginProperties.builder();
    builder.addAll(config.getRawProperties().getProperties());

    if (shouldGetSchema()) {
      // Include source file system properties for schema auto detection
      builder.add(FILE_SYSTEM_PROPERTIES, GSON.toJson(getFileSystemProperties(null)));
    }

    ValidatingInputFormat validatingInputFormat =
      pipelineConfigurer.usePlugin(ValidatingInputFormat.PLUGIN_TYPE, fileFormat, fileFormat, builder.build());
    FormatContext context = new FormatContext(collector, null);
    validateInputFormatProvider(context, fileFormat, validatingInputFormat);

    if (validatingInputFormat != null && shouldGetSchema()) {
      schema = validatingInputFormat.getSchema(context);
    }

    validatePathField(collector, schema);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    String fileFormat = config.getFormatName(); //AVRO, CSV, etc
    ValidatingInputFormat validatingInputFormat;
    try {
      validatingInputFormat = context.newPluginInstance(fileFormat); //TODO $$$
    } catch (InvalidPluginConfigException e) {
      Set<String> properties = new HashSet<>(e.getMissingProperties());
      for (InvalidPluginProperty invalidProperty: e.getInvalidProperties()) {
        properties.add(invalidProperty.getName());
      }
      String errorMessage = String.format("Format '%s' cannot be used because properties %s were not provided or " +
                                            "were invalid when the pipeline was deployed. Set the format to a " +
                                            "different value, or re-create the pipeline with all required properties.",
                                          fileFormat, properties);
      throw new IllegalArgumentException(errorMessage, e);
    }

    //All of this section is the validations to see if you can proceed with the fileformat
    FormatContext formatContext = new FormatContext(collector, context.getInputSchema());
    validateInputFormatProvider(formatContext, fileFormat, validatingInputFormat); //This method calls the validate in our "plugin" provider class
    validatePathField(collector, validatingInputFormat.getSchema(formatContext));
    collector.getOrThrowException(); //This checks if your input provider class has added an error to ITS collector

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Pattern pattern = config.getFilePattern(); //The user has specified - Optional parameter
    if (pattern != null) {
      RegexPathFilter.configure(conf, pattern);
      FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
    }
    FileInputFormat.setInputDirRecursive(job, config.shouldReadRecursively()); // This is provided for us - not related to inputProvider

    Schema schema = config.getSchema(); // This we need to enforce in our inputProvider (via validates)
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName()); //Provided for us
    lineageRecorder.createExternalDataset(schema);

    if (schema != null && schema.getFields() != null) {
      recordLineage(lineageRecorder,
                    schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }

    // set entries here, before FileSystem is used
    for (Map.Entry<String, String> entry : getFileSystemProperties(context).entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    } //Additional Properties we may not need this

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
      FileInputFormat.setMaxInputSplitSize(job, config.getMaxSplitSize()); // This tells the processor parallel split conditions
      inputFormatClass = validatingInputFormat.getInputFormatClassName(); //TODO $$$
      Configuration hConf = job.getConfiguration();
      Map<String, String> inputFormatConfiguration = validatingInputFormat.getInputFormatConfiguration(); //TODO $$
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
    lineageRecorder.recordRead("Read", String.format("Read from %s files.", config.getFormatName()), outputFields);
  }

  private void validateInputFormatProvider(FormatContext context, String fileFormat,
                                           @Nullable ValidatingInputFormat validatingInputFormat) {
    FailureCollector collector = context.getFailureCollector();
    if (validatingInputFormat == null) {
      collector.addFailure(
        String.format("Could not find the '%s' input format.", fileFormat), null)
        .withPluginNotFound(fileFormat, fileFormat, ValidatingInputFormat.PLUGIN_TYPE);
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

  /**
   * Determines if the schema should be auto detected. This method must return false if any of the plugin properties
   * needed to determine the schema is a macro or not present. Otherwise this method should return true.
   * See PLUGIN-508 and CDAP-17473
   */
  protected boolean shouldGetSchema() {
    return true;
  }
}
