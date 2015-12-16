package co.cask.hydrator.plugin.sink;

/**
 *
 */

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.plugin.sink.format.BatchFileFilter;
import co.cask.hydrator.plugin.sink.format.ZipFileInputFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("S3Zip")
@Description("Batch source to use Amazon S3 as a source.")
public class S3BatchSourceZip extends BatchSource<LongWritable, BytesWritable, StructuredRecord> {

  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final String INPUT_REGEX_CONFIG = "input.path.regex";
  public static final String LAST_TIME_READ = "last.time.read";
  public static final String CUTOFF_READ_TIME = "cutoff.read.time";
  public static final String USE_TIMEFILTER = "timefilter";
  protected static final String MAX_SPLIT_SIZE_DESCRIPTION = "Maximum split-size for each mapper in the MapReduce " +
    "Job. Defaults to 128MB.";
  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'.";
  protected static final String TABLE_DESCRIPTION = "Name of the Table that keeps track of the last time files " +
    "were read in. If this is null or empty, the Regex is used to filter filenames.";
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
  private static final Type ARRAYLIST_DATE_TYPE = new TypeToken<ArrayList<Date>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private KeyValueTable table;
  private Date prevHour;
  private String datesToRead;

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSourceZip(S3BatchConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (config.timeTable != null) {
      pipelineConfigurer.createDataset(config.timeTable, KeyValueTable.class, DatasetProperties.EMPTY);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    //SimpleDateFormat needs to be local because it is not threadsafe
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

    //calculate date one hour ago, rounded down to the nearest hour
    prevHour = new Date(context.getLogicalStartTime() - TimeUnit.HOURS.toMillis(1));
    Calendar cal = Calendar.getInstance();
    cal.setTime(prevHour);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    prevHour = cal.getTime();

    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    conf.set("fs.s3n.awsAccessKeyId", config.accessID);
    conf.set("fs.s3n.awsSecretAccessKey", config.accessKey);

    if (config.fileRegex != null) {
      conf.set(INPUT_REGEX_CONFIG, config.fileRegex);
    }
    conf.set(INPUT_NAME_CONFIG, config.path);

    if (config.timeTable != null) {
      table = context.getDataset(config.timeTable);
      datesToRead = Bytes.toString(table.read(LAST_TIME_READ));
      if (datesToRead == null) {
        List<Date> firstRun = Lists.newArrayList(new Date(0));
        datesToRead = GSON.toJson(firstRun, ARRAYLIST_DATE_TYPE);
      }
      List<Date> attempted = Lists.newArrayList(prevHour);
      String updatedDatesToRead = GSON.toJson(attempted, ARRAYLIST_DATE_TYPE);
      if (!updatedDatesToRead.equals(datesToRead)) {
        table.write(LAST_TIME_READ, updatedDatesToRead);
      }
      conf.set(LAST_TIME_READ, datesToRead);
    }

    conf.set(CUTOFF_READ_TIME, dateFormat.format(prevHour));
    job.setInputFormatClass(ZipFileInputFormat.class);
    FileInputFormat.setInputPathFilter(job, BatchFileFilter.class);
    FileInputFormat.addInputPath(job, new Path(config.path));
    FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
  }

  @Override
  public void transform(KeyValue<LongWritable, BytesWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("ts", System.currentTimeMillis())
      .set("body", Bytes.toString(input.getValue().getBytes()))
      .build();
    emitter.emit(output);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (!succeeded && table != null && USE_TIMEFILTER.equals(config.fileRegex)) {
      String lastTimeRead = Bytes.toString(table.read(LAST_TIME_READ));
      List<Date> existing = ImmutableList.of();
      if (lastTimeRead != null) {
        existing = GSON.fromJson(lastTimeRead, ARRAYLIST_DATE_TYPE);
      }
      List<Date> failed = GSON.fromJson(datesToRead, ARRAYLIST_DATE_TYPE);
      failed.add(prevHour);
      failed.addAll(existing);
      table.write(LAST_TIME_READ, GSON.toJson(failed, ARRAYLIST_DATE_TYPE));
    }
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
    public Integer maxSplitSize;

    // sets defaults
    public S3BatchConfig() {
      this.fileSystemProperties = "{}";
      this.fileRegex = ".*";
      this.maxSplitSize = 134217728;
    }
  }
}