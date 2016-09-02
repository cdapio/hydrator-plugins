/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSBatchSink} that stores the data to Google Cloud Storage Bucket.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */

public abstract class GCSBatchSink<KEY_OUT, VAL_OUT> extends ReferenceBatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  public static final String BUCKET_DES = "GCS Bucket used to store the data";
  public static final String PROJECT_ID_DES = "Google Cloud Project ID with access to configured GCS buckets";
  public static final String SERVICE_KEY_FILE_DES = "The Json_Key_File certificate file of the " +
    "service account used for GCS access";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";
  private static final Gson GSON = new Gson();

  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final GCSSinkConfig config;

  protected GCSBatchSink(GCSSinkConfig config) {
    super(config);
    this.config = config;
    // update fileSystemProperties to include ProjectId and JsonKeyFile, so prepareRun can only set
    // fileSystemProperties in configuration, and not deal with projectId and JsonKeyFile separately
    // do not create file system properties if macros were provided unless in a test case
    if (!this.config.containsMacro("fileSystemProperties") && !this.config.containsMacro("JsonKeyFile") &&
      !this.config.containsMacro("ProjectId")) {
      this.config.fileSystemProperties = updateFileSystemProperties(this.config.fileSystemProperties,
                                                                    this.config.projectId, this.config.jsonKey);
    }
  }

  @VisibleForTesting
  GCSSinkConfig getConfig() {
    return config;
  }

  @Override
  public final void prepareRun(BatchSinkContext context) {
    OutputFormatProvider outputFormatProvider = createOutputFormatProvider(context);
    Map<String, String> outputConfig = new HashMap<>(outputFormatProvider.getOutputFormatConfiguration());
    if (config.fileSystemProperties != null) {
      Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
      outputConfig.putAll(properties);
    }
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(
      outputFormatProvider.getOutputFormatClassName(), outputConfig)));
  }

  protected abstract OutputFormatProvider createOutputFormatProvider(BatchSinkContext context);

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties,
                                                   String projectId, String jsonKey) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.gs.project.id", projectId);
    providedProperties.put("google.cloud.auth.service.account.json.keyfile", jsonKey);
    return GSON.toJson(providedProperties);
  }

  /**
   * GCS Sink configuration.
   */
  public static class GCSSinkConfig extends ReferencePluginConfig {

    @Name("BucketKey")
    @Description(BUCKET_DES)
    @Macro
    protected String bucketKey;

    @Name("ProjectId")
    @Description(PROJECT_ID_DES)
    @Macro
    protected String projectId;

    @Name("JsonKeyFile")
    @Description(SERVICE_KEY_FILE_DES)
    @Macro
    protected String jsonKey;

    @Name("BucketDir")
    @Description("the directory inside the bucket where the data is stored. Need to be a new directory.")
    @Macro
    protected String bucketDir;

    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Nullable
    @Macro
    protected String fileSystemProperties;

    public GCSSinkConfig(String referenceName) {
      super(referenceName);
      this.fileSystemProperties = updateFileSystemProperties(null, projectId, jsonKey);
    }

    public GCSSinkConfig(String referenceName, String bucketKey, String projectId,
                         String serviceKeyFile, @Nullable String fileSystemProperties,
                         String bucketDir) {
      super(referenceName);
      this.bucketKey = bucketKey;
      this.projectId = projectId;
      this.jsonKey = serviceKeyFile;
      this.bucketDir = bucketDir;
      this.fileSystemProperties = updateFileSystemProperties(fileSystemProperties, projectId,
                                                             serviceKeyFile);
    }
  }

}
