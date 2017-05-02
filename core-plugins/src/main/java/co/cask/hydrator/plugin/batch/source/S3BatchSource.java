/*
 * Copyright © 2015 Cask Data, Inc.
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
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("S3")
@Description("Batch source to use Amazon S3 as a source.")
public class S3BatchSource extends AbstractFileBatchSource {
  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";
  private static final String AUTHENTICATION_METHOD = "Authentication method to access S3. " +
    "Defaults to Access Credentials. For IAM, URI scheme should be s3a://. (Macro-enabled)";
  private static final String ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  private static final String SECRET_KEY = "fs.s3n.awsSecretAccessKey";
  private static final String ACCESS_CREDENTIALS = "Access Credentials";
  private static final String IAM = "IAM";

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSource(S3BatchConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Config class that contains properties needed for the S3 source.
   */
  public static class S3BatchConfig extends FileSourceConfig {
    private static final Gson GSON = new Gson();

    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. " +
      "The path must start with s3a:// for IAM based authentication.")
    @Macro
    public String path;

    @Description(ACCESS_ID_DESCRIPTION)
    @Macro
    @Nullable
    private final String accessID;

    @Description(ACCESS_KEY_DESCRIPTION)
    @Macro
    @Nullable
    private final String accessKey;

    @Description(AUTHENTICATION_METHOD)
    @Macro
    @Nullable
    private final String authenticationMethod;

    public S3BatchConfig() {
      this(null, null, ACCESS_CREDENTIALS);
    }

    @VisibleForTesting
    public S3BatchConfig(String accessID, String accessKey, String authenticationMethod) {
      this(accessID, accessKey, authenticationMethod, new HashMap<String, String>());
    }

    @VisibleForTesting
    public S3BatchConfig(String accessID, String accessKey, String authenticationMethod,
                         Map<String, String> fileSystemProperties) {
      super();
      this.accessID = accessID;
      this.accessKey = accessKey;
      this.authenticationMethod = authenticationMethod;
      this.fileSystemProperties = GSON.toJson(fileSystemProperties);
    }

    @Override
    protected void validate() {
      super.validate();
      if (authenticationMethod.equalsIgnoreCase(ACCESS_CREDENTIALS)) {
        if (!containsMacro("accessID") && (accessID == null || accessID.isEmpty())) {
          throw new IllegalArgumentException("The Access ID must be specified if " +
                                               "authentication method is Access Credentials.");
        }
        if (!containsMacro("accessKey") && (accessKey == null || accessKey.isEmpty())) {
          throw new IllegalArgumentException("The Access Key must be specified if " +
                                               "authentication method is Access Credentials.");
        }
      } else if (authenticationMethod.equalsIgnoreCase(IAM)) {
        if (!containsMacro("path") && !path.startsWith("s3a://")) {
          throw new IllegalArgumentException("Path must start with s3a:// for IAM based authentication.");
        }
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      if (authenticationMethod.equalsIgnoreCase(ACCESS_CREDENTIALS)) {
        properties.put(ACCESS_KEY, accessID);
        properties.put(SECRET_KEY, accessKey);
      }
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }
}
