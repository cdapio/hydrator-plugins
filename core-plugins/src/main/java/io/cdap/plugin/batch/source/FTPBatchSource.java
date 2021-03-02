/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} that reads from an FTP or SFTP server.
 */
@Plugin(type = "batchsource")
@Name("FTP")
@Description("Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines " +
  "the source server type, either FTP or SFTP.")
public class FTPBatchSource extends AbstractFileSource {
  private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
  private static final String FS_SFTP_IMPL = "fs.sftp.impl";
  private static final String SFTP_FS_CLASS = "org.apache.hadoop.fs.sftp.SFTPFileSystem";
  private static final String FTP_PROTOCOL = "ftp";
  private static final String SFTP_PROTOCOL = "sftp";
  private static final String PATH = "path";
  private static final int DEFAULT_FTP_PORT = 21;
  private static final int DEFAULT_SFTP_PORT = 22;

  public static final Schema SCHEMA = Schema.recordOf("text",
                                                      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Pattern PATTERN_WITHOUT_SPECIAL_CHARACTERS = Pattern.compile("[^A-Za-z0-9]");
  private final FTPBatchSourceConfig config;

  public FTPBatchSource(FTPBatchSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>();
    if (context != null) {
      properties.putAll(config.getFileSystemProperties(context.getFailureCollector()));
    }

    if (!properties.containsKey(FS_SFTP_IMPL)) {
      properties.put(FS_SFTP_IMPL, SFTP_FS_CLASS);
    }
    // Limit the number of splits to 1 since FTPInputStream does not support seek;
    properties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    return properties;
  }

  /**
   * Config class that contains all the properties needed for FTP Batch Source.
   */
  @SuppressWarnings("unused")
  public static class FTPBatchSourceConfig extends PluginConfig implements FileSourceProperties {
    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
    }.getType();

    @Macro
    @Description("Name be used to uniquely identify this source for lineage, annotating metadata, etc.")
    private String referenceName;

    @Macro
    @Description("Path to file(s) to be read. Path is expected to be of the form " +
      "'prefix://username:password@hostname:port/path'.")
    private String path;

    @Macro
    @Nullable
    @Description("Any additional properties to use when reading from the filesystem. "
      + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    private String fileSystemProperties;

    @Nullable
    @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
      + "does not exist. When true, the run will not fail and the source will not generate any output. "
      + "The default value is false.")
    private Boolean ignoreNonExistingFolders;

    @Macro
    @Nullable
    @Description("Regular expression that file names must match in order to be read. "
      + "If no value is given, no file filtering will be done. "
      + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
      + "the regular expression syntax.")
    private String fileRegex;

    @Override
    public void validate() {
      getFileSystemProperties(null);
    }

    @Override
    public void validate(FailureCollector collector) {
      try {
        if (!containsMacro(PATH)) {
          validateAuthenticationInPath(collector);
        }
        getFileSystemProperties(collector);
      } catch (IllegalArgumentException e) {
        collector.addFailure("File system properties must be a valid json.", null)
          .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
      }
    }

    @Override
    public String getReferenceName() {
      return referenceName;
    }

    @Override
    public String getPath() {
      if (authContainsSpecialCharacters()) {
        Path urlInfo;
        String extractedPassword = extractPasswordFromUrl();
        String encodedPassword = URLEncoder.encode(extractedPassword);
        String validatePath = path.replace(extractedPassword, encodedPassword);
        try {
          urlInfo = new Path(validatePath);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("Unable to parse url: %s %s", e.getMessage(), e));
        }
        String host = urlInfo.toUri().getAuthority().substring(urlInfo.toUri().getAuthority().lastIndexOf("@") + 1);
        String user = urlInfo.toUri().getAuthority().split(":")[0];
        String protocol = urlInfo.toUri().getScheme();
        int port = urlInfo.toUri().getPort();
        if (port == -1 && protocol.equals(FTP_PROTOCOL)) {
          port = DEFAULT_FTP_PORT;
        }
        if (port == -1 && protocol.equals(SFTP_PROTOCOL)) {
          port = DEFAULT_SFTP_PORT;
        }
        String cleanHost = host.replace(":" + port, "");
        return urlInfo.toUri().getScheme() + "://" + cleanHost + urlInfo.toUri().getPath();
      }
      return path;
    }

    @Override
    public String getFormatName() {
      return FileFormat.TEXT.name().toLowerCase();
    }

    @Nullable
    @Override
    public Pattern getFilePattern() {
      return null;
    }

    @Override
    public long getMaxSplitSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean shouldAllowEmptyInput() {
      return false;
    }

    @Override
    public boolean shouldReadRecursively() {
      return false;
    }

    @Nullable
    @Override
    public String getPathField() {
      return null;
    }

    @Override
    public boolean useFilenameAsPath() {
      return false;
    }

    @Override
    public boolean skipHeader() {
      return false;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      return SCHEMA;
    }

    @VisibleForTesting
    void configuration(String path, String fileSystemProperties) {
      this.path = path;
      this.fileSystemProperties = fileSystemProperties;
    }

    @VisibleForTesting
    String getPathFromConfig() {
      return path;
    }

    /**
     * This method extracts the password from url
     */
    public String extractPasswordFromUrl() {
      int getLastIndexOfAtSign = path.lastIndexOf("@");
      String authentication = path.substring(0, getLastIndexOfAtSign);
      return authentication.substring(authentication.lastIndexOf(":") + 1);
    }

    /**
     * This method checks if extracted password from url has any special characters
     */
    public boolean authContainsSpecialCharacters() {
      Matcher specialCharacters = PATTERN_WITHOUT_SPECIAL_CHARACTERS.matcher(extractPasswordFromUrl());
      return specialCharacters.find();
    }

    Map<String, String> getFileSystemProperties(FailureCollector collector) {
      HashMap<String, String> fileSystemPropertiesMap = new HashMap<>();
      if (!Strings.isNullOrEmpty(fileSystemProperties)) {
        fileSystemPropertiesMap.putAll(GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE));
      }

      if (!containsMacro(PATH) && authContainsSpecialCharacters()) {
        Path urlInfo = null;
        String extractedPassword = extractPasswordFromUrl();
        String encodedPassword = URLEncoder.encode(extractedPassword);
        String validatePath = path.replace(extractedPassword, encodedPassword);
        try {
          urlInfo = new Path(validatePath);
        } catch (Exception e) {
          if (collector != null) {
            collector.addFailure(String.format("Unable to parse url: %s.", path),
                                 "Path is expected to be of the form " +
                                   "prefix://username:password@hostname:port/path")
              .withConfigProperty(PATH).withStacktrace(e.getStackTrace());
            collector.getOrThrowException();
          } else {
            throw new IllegalArgumentException(String.format("Unable to parse url: %s. %s", path,
                                                             "Path is expected to be of the form " +
                                                               "prefix://username:password@hostname:port/path"));
          }
        }
        // After encoding the url, the format should look like:
        // ftp://kimi:42%4067%5Dgfuss@192.168.0.179:21/kimi-look-here.txt
        int port = urlInfo.toUri().getPort();
        String host = urlInfo.toUri().getAuthority().substring(urlInfo.toUri().getAuthority().lastIndexOf("@") + 1);
        String user = urlInfo.toUri().getAuthority().split(":")[0];
        if (urlInfo.toUri().getScheme().equals(FTP_PROTOCOL)) {
          port = (port == -1) ? DEFAULT_FTP_PORT : port;
          String cleanHostFTP = host.replace(":" + port, "");
          fileSystemPropertiesMap.put("fs.ftp.host", cleanHostFTP);
          fileSystemPropertiesMap.put(String.format("fs.ftp.user.%s", cleanHostFTP), user);
          fileSystemPropertiesMap.put(String.format("fs.ftp.password.%s", cleanHostFTP), extractedPassword);
          fileSystemPropertiesMap.put("fs.ftp.host.port", String.valueOf(port));
        } else {
          port = (port == -1) ? DEFAULT_SFTP_PORT : port;
          String cleanHostSFTP = host.replace(":" + port, "");
          fileSystemPropertiesMap.put("fs.sftp.host", cleanHostSFTP);
          fileSystemPropertiesMap.put(String.format("fs.sftp.user.%s", cleanHostSFTP), user);
          fileSystemPropertiesMap.put(String.format("fs.sftp.password.%s.%s", cleanHostSFTP, user),
                                      extractedPassword);
          fileSystemPropertiesMap.put("fs.sftp.host.port", String.valueOf(port));
        }
      }
      return fileSystemPropertiesMap;
    }

    /**
     * This method checks if the path contains authentication and it's in a valid form
     * If the user doesn't provide the authentication in path, we will raise a failure
     * Example of missing authentication in url: ftp://192.168.0.179/kimi-look-here.txt
     */
    private void validateAuthenticationInPath(FailureCollector collector) {
      int getLastIndexOfAtSign = path.lastIndexOf("@");
      if (getLastIndexOfAtSign == -1) {
        collector.addFailure(String.format("Missing authentication in url: %s.", path),
                             "Path is expected to be of the form " +
                               "prefix://username:password@hostname:port/path")
          .withConfigProperty(PATH);
        collector.getOrThrowException();
      }
    }
  }
}
