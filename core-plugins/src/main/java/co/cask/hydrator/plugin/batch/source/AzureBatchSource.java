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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link BatchSource} for Azure Blob Store.
 */
@Plugin(type = "batchsource")
@Name("AzureBlobStore")
@Description("Batch source to read from Azure Blob Storage.")
public class AzureBatchSource extends FileBatchSource {

  @SuppressWarnings("unused")
  private final AzureBatchConfig config;

  public AzureBatchSource(AzureBatchConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Plugin config for {@link AzureBatchSource}.
   */
  public static class AzureBatchConfig extends FileBatchConfig {
    @Description("The Microsoft Azure Storage account to use.")
    @Macro
    private String account;

    @Description("The container to use on the specified Microsoft Azure Storage account.")
    @Macro
    private String container;

    @Description("The storage key for the specified container on the specified Azure Storage account. Must be a " +
      "valid base64 encoded storage key provided by Microsoft Azure.")
    @Macro
    private String storageKey;

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("fs.defaultFS", String.format("wasb://%s@%s/", container, account));
      properties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
      properties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
      properties.put(String.format("fs.azure.account.key.%s", account), storageKey);
      return properties;
    }
  }
}
