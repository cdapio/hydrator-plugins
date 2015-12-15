package co.cask.hydrator.plugin.sink;

/**
 *
 */

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.source.FileBatchSource;
import co.cask.hydrator.plugin.sink.format.ZipFileInputFormat;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("S3Zip")
@Description("Batch source to use Amazon S3 as a source.")
public class S3BatchSourceZip extends FileBatchSource {

  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'.";
  protected static final String TABLE_DESCRIPTION = "Name of the Table that keeps track of the last time files " +
    "were read in. If this is null or empty, the Regex is used to filter filenames.";
  protected static final String INPUT_FORMAT_CLASS_DESCRIPTION = "Name of the input format class, which must be a " +
    "subclass of FileInputFormat. Defaults to CombineTextInputFormat.";
  protected static final String REGEX_DESCRIPTION = "Regex to filter out filenames in the path. " +
    "To use the TimeFilter, input \"timefilter\". The TimeFilter assumes that it " +
    "is reading in files with the File log naming convention of 'YYYY-MM-DD-HH-mm-SS-Tag'. The TimeFilter " +
    "reads in files from the previous hour if the field 'timeTable' is left blank. If it's currently " +
    "2015-06-16-15 (June 16th 2015, 3pm), it will read in files that contain '2015-06-16-14' in the filename. " +
    "If the field 'timeTable' is present, then it will read in files that have not yet been read. Defaults to '.*', " +
    "which indicates that no files will be filtered.";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";


  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSourceZip(S3BatchConfig config) {
    // update fileSystemProperties with S3 properties, so FileBatchSource.prepareRun can use them
    super(new FileBatchConfig(config.path, config.fileRegex, config.timeTable, ZipFileInputFormat.class.toString(),
                              updateFileSystemProperties(
                                config.fileSystemProperties, config.accessID, config.accessKey
                              ),
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
  public static class S3BatchConfig extends PluginConfig {


    @Description(PATH_DESCRIPTION)
    public String path;

    @Nullable
    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    public String fileSystemProperties;

    @Nullable
    @Description(REGEX_DESCRIPTION)
    public String fileRegex;

    @Nullable
    @Description(TABLE_DESCRIPTION)
    public String timeTable;

    @Description(ACCESS_ID_DESCRIPTION)
    private String accessID;

    @Description(ACCESS_KEY_DESCRIPTION)
    private String accessKey;

    @Nullable
    @Description(MAX_SPLIT_SIZE_DESCRIPTION)
    public String maxSplitSize;


    public S3BatchConfig(String accessID, String accessKey, String path, @Nullable String regex,
                         @Nullable String timeTable,
                         @Nullable String fileSystemProperties, @Nullable String maxSplitSize) {

      this.accessID = accessID;
      this.accessKey = accessKey;
      this.path = path;
      this.fileRegex = regex;
      this.fileSystemProperties = updateFileSystemProperties(fileSystemProperties, accessID, accessKey);
      this.maxSplitSize = maxSplitSize;
    }
  }
}