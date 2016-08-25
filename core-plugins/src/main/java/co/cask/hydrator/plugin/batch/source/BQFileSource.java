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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("BQFile")
@Description("Batch source to use bigquery as a source.")
public class BQFileSource extends FileBatchSource {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  private static final String PROJECT_ID_DES = "Project Id this bigquery uses";
  private static final String JSON_KEY_DES = "JSON key file path";
  private static final String IMPORT_QUERY_DES = "Import query which include the target table, this is optional";
  private static final String TEMP_BUCKET_PATH_DESC = "The tempory bucket path which store the query or table content";
  private static final String FULLY_INPUT_TABLE_ID_DESC = "fully input table id";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSource(S3BatchConfig config) {
    // update fileSystemProperties with S3 properties, so FileBatchSource.prepareRun can use them
    super(new FileBatchConfig(config.referenceName, config.path, config.fileRegex, config.timeTable,
                              config.inputFormatClass, updateFileSystemProperties(
      config.fileSystemProperties, config.accessID, config.accessKey),
                              config.maxSplitSize));
    this.config = config;
  }

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties, String accessID,
                                                   String accessKey) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.s3n.awsAccessKeyId", accessID);
    providedProperties.put("fs.s3n.awsSecretAccessKey", accessKey);
    return GSON.toJson(providedProperties);
  }

  /**
   * Config class that contains properties needed for the S3 source.
   */
  public static class S3BatchConfig extends FileBatchConfig {
    @Description(ACCESS_ID_DESCRIPTION)
    @Macro
    private final String accessID;

    @Description(ACCESS_KEY_DESCRIPTION)
    @Macro
    private final String accessKey;

    public S3BatchConfig(String referenceName, String accessID, String accessKey, String path) {
      this(referenceName, accessID, accessKey, path, null, null, null, null, null);
    }

    public S3BatchConfig(String referenceName, String accessID, String accessKey, String path, @Nullable String regex,
                         @Nullable String timeTable, @Nullable String inputFormatClass,
                         @Nullable String fileSystemProperties, @Nullable Long maxSplitSize) {
      super(referenceName, path, regex, timeTable, inputFormatClass,
            updateFileSystemProperties(fileSystemProperties, accessID, accessKey), maxSplitSize);
      this.accessID = accessID;
      this.accessKey = accessKey;
    }
  }
}
