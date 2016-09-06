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
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
  public static final String PROJECT_ID_DES = "Google Cloud Project ID with access to configured GCS buckets.";
  public static final String SERVICE_KEY_FILE_DES = "The JSON certificate file of the " +
    "service account used for GCS access.";
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
    if (!this.config.containsMacro("fileSystemProperties") && !this.config.containsMacro("jsonKeyFile") &&
      !this.config.containsMacro("projectId")) {
      this.config.fileSystemProperties = this.config.getFileSystemProperties(this.config.fileSystemProperties,
                                                                             this.config.projectId,
                                                                             this.config.jsonKeyFile);
    }
  }

  @Override
  public final void prepareRun(BatchSinkContext context) {
    OutputFormatProvider outputFormatProvider = createOutputFormatProvider(context, config.fileSystemProperties);
    context.addOutput(Output.of(config.referenceName, outputFormatProvider));
  }

  protected abstract OutputFormatProvider createOutputFormatProvider(BatchSinkContext context,
                                                                     String fileSystemProperties);

  /**
   * GCS Sink configuration.
   */
  public static class GCSSinkConfig extends ReferencePluginConfig {

    @Name("bucketKey")
    @Description("GCS Bucket to use to store the data")
    @Macro
    protected String bucketKey;

    @Name("projectId")
    @Description(PROJECT_ID_DES)
    @Macro
    protected String projectId;

    @Name("jsonKeyFile")
    @Description(SERVICE_KEY_FILE_DES)
    @Macro
    protected String jsonKeyFile;

    @Name("pathToStore")
    @Description("path to store inside bucket")
    @Macro
    protected String bucketDir;

    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Nullable
    @Macro
    protected String fileSystemProperties;

    public String getFileSystemProperties(@Nullable String fileSystemProperties,
                                          String projectId, String jsonKeyFile) {
      Map<String, String> providedProperties;
      if (fileSystemProperties == null) {
        providedProperties = new HashMap<>();
      } else {
        providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
      }
      providedProperties.put("fs.gs.project.id", projectId);
      providedProperties.put("google.cloud.auth.service.account.json.keyfile", jsonKeyFile);
      providedProperties.put(FileOutputFormat.OUTDIR,
                             String.format("gs://%s/%s", bucketKey, bucketDir));
      return GSON.toJson(providedProperties);
    }

    public GCSSinkConfig(String referenceName, String bucketKey, String projectId,
                         String serviceKeyFile, @Nullable String fileSystemProperties,
                         String bucketDir) {
      super(referenceName);
      this.bucketKey = bucketKey;
      this.projectId = projectId;
      this.jsonKeyFile = serviceKeyFile;
      this.bucketDir = bucketDir;
      this.fileSystemProperties = getFileSystemProperties(fileSystemProperties, projectId,
                                                          serviceKeyFile);
    }
  }
}
