/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.AvroToStructuredTransformer;
import co.cask.hydrator.plugin.common.BatchFileFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
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
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "batchsource")
@Name("Parquet")
@Description("Batch source for Parquet Files")
public class ParquetFileSource extends ReferenceBatchSource<NullWritable, GenericRecord, StructuredRecord> {
  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final String INPUT_REGEX_CONFIG = "input.path.regex";
  public static final String LAST_TIME_READ = "last.time.read";
  public static final String CUTOFF_READ_TIME = "cutoff.read.time";
  public static final String USE_TIMEFILTER = "timefilter";

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();
  private co.cask.cdap.api.data.schema.Schema outputSchema;

  protected static final String MAX_SPLIT_SIZE_DESCRIPTION = "Maximum split-size for each mapper in the MapReduce " +
    "Job. Defaults to 128MB.";
  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come" +
    " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where" +
    " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'. The path uses " +
    "filename expansion (globbing) to read files.";
  protected static final String TABLE_DESCRIPTION = "Name of the Table that keeps track of the last time files " +
    "were read in. If this is null or empty, the Regex is used to filter filenames.";
  protected static final String INPUT_FORMAT_CLASS_DESCRIPTION = "Name of the input format class, which must be a " +
    "subclass of FileInputFormat. Defaults to CombineTextInputFormat.";
  protected static final String REGEX_DESCRIPTION = "Regex to filter out files in the path. It accepts regular " +
    "expression which is applied to the complete path and returns the list of files that match the specified pattern." +
    "To use the TimeFilter, input \"timefilter\". The TimeFilter assumes that it " +
    "is reading in files with the File log naming convention of 'YYYY-MM-DD-HH-mm-SS-Tag'. The TimeFilter " +
    "reads in files from the previous hour if the field 'timeTable' is left blank. If it's currently " +
    "2015-06-16-15 (June 16th 2015, 3pm), it will read in files that contain '2015-06-16-14' in the filename. " +
    "If the field 'timeTable' is present, then it will read in files that have not yet been read. Defaults to '.*', " +
    "which indicates that no files will be filtered.";
  protected static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_DATE_TYPE = new TypeToken<ArrayList<Date>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  @VisibleForTesting
  static final long DEFAULT_MAX_SPLIT_SIZE = 134217728;

  private static final Logger LOG = LoggerFactory.getLogger(FileBatchSource.class);
  private final ParquetFileConfig config;
  private KeyValueTable table;
  private Date prevHour;
  private String datesToRead;
  private Path path;
  private FileSystem fs;

  public ParquetFileSource(ParquetFileConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    if (!config.containsMacro("timeTable") && config.timeTable != null) {
      pipelineConfigurer.createDataset(config.timeTable, KeyValueTable.class, DatasetProperties.EMPTY);
    }

    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.schema), "Schema must be specified.");
    try {
      outputSchema = co.cask.cdap.api.data.schema.Schema.parseJson(config.schema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputSchema = Schema.parseJson(config.schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // Need to create dataset now if macro was provided at configure time
    if (config.timeTable != null && !context.datasetExists(config.timeTable)) {
      context.createDataset(config.timeTable, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }

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

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
    //noinspection ConstantConditions
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    conf.set(INPUT_REGEX_CONFIG, config.fileRegex);

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
    FileInputFormat.setInputPathFilter(job, BatchFileFilter.class);
    FileInputFormat.setInputDirRecursive(job, config.recursive);

    FileSystem pathFileSystem = FileSystem.get(new Path(config.path).toUri(), conf);
    FileStatus[] fileStatus = pathFileSystem.globStatus(new Path(config.path));
    fs = FileSystem.get(conf);

    if (fileStatus == null && config.ignoreNonExistingFolders) {
      path = fs.getWorkingDirectory().suffix("/tmp/tmp.txt");
      LOG.warn(String.format("File/Folder specified in %s does not exists. Setting input path to %s.", config.path,
                             path));
      fs.createNewFile(path);
      conf.set(INPUT_NAME_CONFIG, path.toUri().getPath());
      FileInputFormat.addInputPath(job, path);
    } else {
      conf.set(INPUT_NAME_CONFIG, new Path(config.path).toString());
      FileInputFormat.addInputPath(job, new Path(config.path));
    }

    if (config.maxSplitSize != null) {
      FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
    }
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(config.inputFormatClass, conf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, GenericRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(recordTransformer.transform(input.getValue(), outputSchema));
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
    try {
      if (path != null && fs.exists(path.getParent())) {
        fs.delete(path.getParent(), true);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Error deleting temporary file. %s.", e.getMessage()), e);
    }
  }

  @VisibleForTesting
  ParquetFileConfig getConfig() {
    return config;
  }

  /**
   * Config class that contains all the properties needed for the file source.
   */
  public static class ParquetFileConfig extends ReferencePluginConfig {
    @Description(PATH_DESCRIPTION)
    @Macro
    public String path;

    @Description("The Parquet schema of the record being read from the source as a JSON Object.")
    private String schema;

    @Nullable
    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Macro
    public String fileSystemProperties;

    @Nullable
    @Description(REGEX_DESCRIPTION)
    @Macro
    public String fileRegex;

    @Nullable
    @Description(TABLE_DESCRIPTION)
    @Macro
    public String timeTable;

    @Nullable
    @Description(INPUT_FORMAT_CLASS_DESCRIPTION)
    @Macro
    public String inputFormatClass;

    @Nullable
    @Description(MAX_SPLIT_SIZE_DESCRIPTION)
    @Macro
    public Long maxSplitSize;

    @Nullable
    @Description("Identify if path needs to be ignored or not, for case when directory or file does not exists. If " +
      "set to true it will treat the not present folder as zero input and log a warning. Default is false.")
    public Boolean ignoreNonExistingFolders;

    @Nullable
    @Description("Boolean value to determine if files are to be read recursively from the path. Default is false.")
    public Boolean recursive;

    public ParquetFileConfig() {
      super("");
      this.fileSystemProperties = GSON.toJson(ImmutableMap.<String, String>of());
      this.fileRegex = ".*";
      this.inputFormatClass = AvroParquetInputFormat.class.getName();
      this.maxSplitSize = DEFAULT_MAX_SPLIT_SIZE;
      this.ignoreNonExistingFolders = false;
      this.recursive = false;
    }

    public ParquetFileConfig(String referenceName, String path, @Nullable String fileRegex, @Nullable String timeTable,
                             @Nullable String inputFormatClass, @Nullable String fileSystemProperties,
                             @Nullable Long maxSplitSize, @Nullable Boolean ignoreNonExistingFolders,
                             @Nullable Boolean recursive) {
      super(referenceName);
      this.path = path;
      this.fileSystemProperties = fileSystemProperties == null ? GSON.toJson(ImmutableMap.<String, String>of()) :
        fileSystemProperties;
      this.fileRegex = fileRegex == null ? ".*" : fileRegex;
      // There is no default for timeTable, the code handles nulls
      this.timeTable = timeTable;
      this.inputFormatClass = inputFormatClass == null ? CombineTextInputFormat.class.getName() : inputFormatClass;
      this.maxSplitSize = maxSplitSize == null ? DEFAULT_MAX_SPLIT_SIZE : maxSplitSize;
      this.ignoreNonExistingFolders = ignoreNonExistingFolders == null ? false : ignoreNonExistingFolders;
      this.recursive = recursive == null ? false : recursive;
    }
  }
}
