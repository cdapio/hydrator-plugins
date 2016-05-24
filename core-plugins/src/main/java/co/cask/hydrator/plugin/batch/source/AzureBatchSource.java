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
 * {@link BatchSource} for Azure Blob Store.
 */
@Plugin(type = "batchsource")
@Name("AzureBlobStore")
@Description("Batch source to read from Azure Blob Storage.")
public class AzureBatchSource extends FileBatchSource {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @SuppressWarnings("unused")
  private final AzureBatchConfig config;

  public AzureBatchSource(AzureBatchConfig config) {
    super(new FileBatchConfig(config.referenceName, config.path, config.fileRegex, config.timeTable,
                              config.inputFormatClass, updateFileSystemProperties(
                                config.fileSystemProperties, config.account, config.container, config.storageKey),
                              config.maxSplitSize));
    this.config = config;
  }

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties, String account,
                                                   String container, String storageKey) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.defaultFS", String.format("wasb://%s@%s/", container, account));
    providedProperties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
    providedProperties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
    providedProperties.put(String.format("fs.azure.account.key.%s", account), storageKey);
    return GSON.toJson(providedProperties);
  }

  /**
   * Plugin config for {@link AzureBatchSource}.
   */
  public static class AzureBatchConfig extends FileBatchConfig {
    @Description("The Microsoft Azure Storage account to use.")
    private final String account;

    @Description("The container to use on the specified Microsoft Azure Storage account.")
    private final String container;

    @Description("The storage key for the specified container on the specified Azure Storage account. Must be a " +
      "valid base64 encoded storage key provided by Microsoft Azure.")
    private final String storageKey;

    public AzureBatchConfig(String referenceName, String path, String account, String container, String storageKey) {
      this(referenceName, path, account, container, storageKey, null, null, null, null, null);
    }

    public AzureBatchConfig(String referenceName, String path, String account, String container, String storageKey,
                            @Nullable String regex, @Nullable String timeTable, @Nullable String inputFormatClass,
                            @Nullable String fileSystemProperties, @Nullable Long maxSplitSize) {
      super(referenceName, path, regex, timeTable, inputFormatClass,
            updateFileSystemProperties(fileSystemProperties, account, container, storageKey),
            maxSplitSize);
      this.account = account;
      this.container = container;
      this.storageKey = storageKey;
      this.path = path;
    }
  }
}
