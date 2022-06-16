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

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseEntityTypeInfo;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SamplePropertyField;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.ThrowableFunction;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.SchemaDetector;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final Gson GSON = new Gson();
  private final T config;

  protected static final String PLUGIN_NAME_PROPERTY_KEY = "_pluginName";

  static final String SAMPLE_FORMAT_KEY = "format";
  static final String SAMPLE_DELIMITER_KEY = "delimiter";
  static final String SAMPLE_FILE_ENCODING_KEY = "fileEncoding";
  static final String SAMPLE_ENABLE_QUOTED_VALUES_KEY = "enableQuotedValues";
  static final String SAMPLE_FILE_ENCODING_DEFAULT = "UTF-8";
  static final String SAMPLE_SKIP_HEADER_KEY = "skipHeader";
  static final String SAMPLE_SCHEMA = "schema";

  static final List<String> SAMPLE_FIELD_NAMES = Arrays.asList(SAMPLE_FORMAT_KEY, SAMPLE_DELIMITER_KEY,
                                                               SAMPLE_FILE_ENCODING_KEY, SAMPLE_SKIP_HEADER_KEY,
                                                               SAMPLE_ENABLE_QUOTED_VALUES_KEY, SAMPLE_SCHEMA);

  private final Set<BrowseEntityTypeInfo> sampleProperties;

  protected AbstractFileConnector(T config) {
    this.config = config;
    this.sampleProperties = new HashSet<>();
  }

  /**
   * Adds available sample fields for each different entityType.
   *
   * @param entityType
   * @param fileSourceClass FileSourceProperties class containing the configuration and description of the properties.
   */
  protected void initSampleFields(String entityType, Class<? extends FileSourceProperties> fileSourceClass) {
    initSampleFields(entityType, fileSourceClass, Collections.emptyMap());
  }

  /**
   * Adds available sample fields for each different entityType.
   *
   * @param entityType
   * @param fileSourceClass FileSourceProperties class containing the configuration and description of the properties.
   * @param additionalProperties Map with extra properties to add to the sample properties.
   */
  protected void initSampleFields(String entityType, Class<? extends FileSourceProperties> fileSourceClass,
                                  Map<String, String> additionalProperties) {
    List<Field> sourceFields = new ArrayList<>();

    for (String sourceFieldName : SAMPLE_FIELD_NAMES) {
      Class<?> fileClass = fileSourceClass;
      while (fileClass != null) {
        try {
          sourceFields.add(fileClass.getDeclaredField(sourceFieldName));
          break;
        } catch (NoSuchFieldException e) {
          // Check if it's defined in its super class.
          fileClass = fileClass.getSuperclass();
        }
        // Skip sample option as it's not implemented by the source
      }
    }

    List<SamplePropertyField> browseEntityTypeInfoList = new ArrayList<>();
    for (Field field: sourceFields) {
      browseEntityTypeInfoList.add(
        new SamplePropertyField(field.getName(),
                                field.getDeclaredAnnotation(Description.class).value()));
    }

    for (Map.Entry<String, String> property: additionalProperties.entrySet()) {
      browseEntityTypeInfoList.add(
        new SamplePropertyField(property.getKey(), property.getValue())
      );
    }

    sampleProperties.add(new BrowseEntityTypeInfo(entityType, browseEntityTypeInfoList));
  }

  @Override
  public InputFormatProvider getInputFormatProvider(ConnectorContext context,
                                                    SampleRequest sampleRequest) throws IOException {
    String fullPath = getFullPath(sampleRequest.getPath());
    Map<String, String> sampleRequestProperties = sampleRequest.getProperties();
    ValidatingInputFormat inputFormat =
      getValidatingInputFormat(context, fullPath, sampleRequestProperties);
    FormatContext formatContext = new FormatContext(context.getFailureCollector(), null);
    inputFormat.validate(formatContext);
    context.getFailureCollector().getOrThrowException();

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
    ValidatingInputFormat format = getValidatingInputFormat(context, path, connectorSpecRequest.getProperties());
    FormatContext formatContext = new FormatContext(context.getFailureCollector(), null);
    Schema schema = format.getSchema(formatContext);
    if (schema == null) {
      SchemaDetector schemaDetector = new SchemaDetector(format);
      schema = schemaDetector.detectSchema(path, formatContext, getFileSystemProperties(path));
    }
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
    //no op
  }

  /**
   * Adds default sampling values to entity.
   *
   * @param entity entity to which add the sampling default values
   * @param fileName name of the file. Used to guess correct format based on extension.
   */
  protected void addBrowseSampleDefaultValues(BrowseEntity.Builder entity, String fileName) {
    entity.addProperty(SAMPLE_FORMAT_KEY, BrowseEntityPropertyValue.builder(
      FileTypeDetector.detectFileFormat(FileTypeDetector.detectFileType(fileName)).name().toLowerCase(),
      BrowseEntityPropertyValue.PropertyType.SAMPLE_DEFAULT).build());
    entity.addProperty(SAMPLE_FILE_ENCODING_KEY, BrowseEntityPropertyValue.builder(
      SAMPLE_FILE_ENCODING_DEFAULT, BrowseEntityPropertyValue.PropertyType.SAMPLE_DEFAULT).build());
  }

  /**
   *
   * @return Set of available sample properties
   */
  protected Set<BrowseEntityTypeInfo> getSampleProperties() {
    return sampleProperties;
  }

  /**
   * Receives a request and returns a map with only the valid sample properties.
   *
   * @param request Request containing the sample properties.
   * @return a Map with chosen sample properties
   */
  protected Map<String, String> getAdditionalSpecProperties(ConnectorSpecRequest request) {
    Map<String, String> requestProperties = request.getProperties();
    Map<String, String> sampleProperties = new HashMap<>();
    for (String sampleKey : SAMPLE_FIELD_NAMES) {
      if (requestProperties.containsKey(sampleKey)) {
        sampleProperties.put(sampleKey, requestProperties.get(sampleKey));
      }
    }
    return sampleProperties;
  }

  private ValidatingInputFormat getValidatingInputFormat(ConnectorContext context, String path,
                                                 Map<String, String> sampleProperties) throws IOException {
    PluginProperties.Builder builder = PluginProperties.builder();
    builder.addAll(sampleProperties);

    String fileType = FileTypeDetector.detectFileType(path);
    if (!FileTypeDetector.isSampleable(fileType)) {
      throw new IllegalArgumentException(String.format("The given path %s cannot be sampled.", path));
    }

    builder.add("path", path);
    String format;
    if (sampleProperties.containsKey("format")) {
      format = sampleProperties.get("format");
    } else {
      format = FileTypeDetector.detectFileFormat(fileType).name().toLowerCase();
      builder.add("format", format);
    }

    if (FileFormat.TEXT.name().equalsIgnoreCase(format)) {
      builder.add("schema", DEFAULT_TEXT_SCHEMA);
    }
    builder.addAll(config.getProperties().getProperties());
    builder.addAll(getFileSystemProperties(path));

    // Adding FileSystem properties under its own entry as its used as a config parameter in the plugin.
    builder.add("fileSystemProperties", GSON.toJson(getFileSystemProperties(path)));
    ValidatingInputFormat inputFormat = context.getPluginConfigurer().usePlugin(
      ValidatingInputFormat.PLUGIN_TYPE, format, UUID.randomUUID().toString(), builder.build());

    if (inputFormat == null) {
      throw new IOException(
        String.format("Unsupported file format %s on path %s", format, path));
    }
    return inputFormat;
  }

}
