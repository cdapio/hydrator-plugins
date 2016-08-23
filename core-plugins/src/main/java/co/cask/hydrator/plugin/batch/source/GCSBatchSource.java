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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSource;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Google Cloud Storage.
 */
@Plugin(type = "batchsource")
@Name("GCS")
@Description("Batch source to use Amazon S3 as a source.")
public class GCSBatchSource extends FileBatchSource {
  private static final String PROJECT_ID_DESCRIPTION = "Google Cloud Storage Project ID";
  private static final String JSON_KEYFILE_DESCRIPTION = "Json key file path";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @SuppressWarnings("unused")
  private final GCSBatchConfig config;

  public GCSBatchSource(GCSBatchConfig config) {
    // update fileSystemProperties with GCS properties, so FileBatchSource.prepareRun can use them
    super(new FileBatchConfig(config.referenceName, config.path, config.fileRegex, config.timeTable,
                              config.inputFormatClass, updateFileSystemProperties(
      config.fileSystemProperties, config.projectID, config.jsonKeyFile),
                              config.maxSplitSize));
    this.config = config;
  }

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties, String projectId,
                                                   String jsonKeyFile) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.gs.project.id", projectId);
    providedProperties.put("google.cloud.auth.service.account.json.keyfile", jsonKeyFile);
    return GSON.toJson(providedProperties);
  }

  /**
   * Config class that contains properties needed for the S3 source.
   */
  public static class GCSBatchConfig extends FileBatchConfig {
    @Name("ProjectId")
    @Description(PROJECT_ID_DESCRIPTION)
    @Macro
    private final String projectID;

    @Name("JsonKeyFilePath")
    @Description(JSON_KEYFILE_DESCRIPTION)
    @Macro
    private final String jsonKeyFile;


    public GCSBatchConfig(String referenceName, String projectID, String jsonKeyFile, String path) {
      this(referenceName, projectID, jsonKeyFile, path, null, null, null, null, null);
    }

    public GCSBatchConfig(String referenceName, String projectID, String jsonKeyFile, String path,
                         @Nullable String regex, @Nullable String timeTable, @Nullable String inputFormatClass,
                         @Nullable String fileSystemProperties, @Nullable Long maxSplitSize) {
      super(referenceName, path, regex, timeTable, inputFormatClass,
            updateFileSystemProperties(fileSystemProperties, projectID, jsonKeyFile), maxSplitSize);
      this.projectID = projectID;
      this.jsonKeyFile = jsonKeyFile;
    }
  }
}
