/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.format.connector;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.ThrowableFunction;
import io.cdap.plugin.format.FileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Common logic for file related connector. Currently this class includes some hack on guessing the file format.
 *
 * @param <T> type of config
 */
public abstract class AbstractFileConnector<T extends PluginConfig>
  implements BatchConnector<NullWritable, StructuredRecord> {
  // we do not want offset field in the schema
  private static final String DEFAULT_TEXT_SCHEMA =
    Schema.recordOf("text", Schema.Field.of("body", Schema.of(Schema.Type.STRING))).toString();

  private final T config;

  protected AbstractFileConnector(T config) {
    this.config = config;
  }

  @Override
  public InputFormatProvider getInputFormatProvider(ConnectorContext context,
                                                    SampleRequest sampleRequest) throws IOException {
    String fullPath = getFullPath(sampleRequest.getPath());
    ValidatingInputFormat inputFormat = getValidatingInputFormat(context, fullPath);

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    // set entries here, before FileSystem is used
    for (Map.Entry<String, String> entry : getFileSystemProperties(fullPath).entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    Path path = new Path(fullPath);
    // need this to load the extra class loader to avoid ClassNotFoundException for the file system
    FileSystem fs = JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                                       f -> FileSystem.get(path.toUri(), conf));

    FileStatus[] fileStatus = fs.globStatus(path);

    if (fileStatus == null) {
      throw new IOException(String.format("Input path %s does not exist", path));
    }

    // add input path also needs to create the file system so need to wrap it
    JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                       (ThrowableFunction<JobContext, Void, IOException>) t -> {
      FileInputFormat.addInputPath(job, path);
      return null;
    });

    String inputFormatClassName = inputFormat.getInputFormatClassName();
    Configuration hConf = job.getConfiguration();
    Map<String, String> inputFormatConfiguration = inputFormat.getInputFormatConfiguration();
    for (Map.Entry<String, String> propertyEntry : inputFormatConfiguration.entrySet()) {
      hConf.set(propertyEntry.getKey(), propertyEntry.getValue());
    }

    // set entries here again, in case anything set by PathTrackingInputFormat should be overridden
    for (Map.Entry<String, String> entry : getFileSystemProperties(fullPath).entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    return new SourceInputFormatProvider(inputFormatClassName, conf);
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    // no-op
  }

  @Override
  public StructuredRecord transform(NullWritable key, StructuredRecord val) {
    return val;
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext context,
                                    ConnectorSpecRequest connectorSpecRequest) throws IOException {
    ConnectorSpec.Builder builder = ConnectorSpec.builder();

    String path = getFullPath(connectorSpecRequest.getPath());
    ValidatingInputFormat inputFormat = getValidatingInputFormat(context, path);
    // TODO: CDAP-18060 in 6.5 this will only have text and blob that will not access the file system,
    //  to support other formats, we need to ensure get schema works
    Schema schema = inputFormat.getSchema(new FormatContext(context.getFailureCollector(), null));
    builder.setSchema(schema);
    setConnectorSpec(connectorSpecRequest, builder);
    return builder.build();
  }

  /**
   * Return the full path including the scheme for the connector
   */
  protected String getFullPath(String path) {
    return path;
  }

  /**
   * Override this to provide any additional Configuration properties that are required by the FileSystem.
   * For example, if the FileSystem requires setting properties for credentials, those should be returned by
   * this method.
   *
   * @param path the path for the file system, some file system needs this path, for example, gcs needs the path to
   *             determine the bucket name
   */
  protected Map<String, String> getFileSystemProperties(String path) {
    return Collections.emptyMap();
  }

  /**
   * Override this method to provide related plugins, properties or schema
   *
   * @param request the spec generation request
   * @param builder the builder of the spec
   */
  protected void setConnectorSpec(ConnectorSpecRequest request, ConnectorSpec.Builder builder) {
    // no-op
  }

  private ValidatingInputFormat getValidatingInputFormat(ConnectorContext context, String path) throws IOException {
    String fileType = FileTypeDetector.detectFileType(path);
    if (!FileTypeDetector.isSampleable(fileType)) {
      throw new IllegalArgumentException(String.format("The given path %s cannot be sampled.", path));
    }

    FileFormat format = FileTypeDetector.detectFileFormat(fileType);
    PluginProperties.Builder builder = PluginProperties.builder();
    builder.add("path", path);
    if (format.equals(FileFormat.TEXT)) {
      builder.add("schema", DEFAULT_TEXT_SCHEMA);
    }
    builder.addAll(config.getProperties().getProperties());
    builder.addAll(getFileSystemProperties(path));

    ValidatingInputFormat inputFormat = context.getPluginConfigurer().usePlugin(
      ValidatingInputFormat.PLUGIN_TYPE, format.name().toLowerCase(), UUID.randomUUID().toString(), builder.build());

    if (inputFormat == null) {
      throw new IOException(
        String.format("Unsupported file format %s on path %s", format, path));
    }
    return inputFormat;
  }
}
