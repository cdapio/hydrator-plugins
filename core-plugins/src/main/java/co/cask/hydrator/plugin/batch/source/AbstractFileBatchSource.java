/*
 *
 *  * Copyright © 2017 Cask Data, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License. You may obtain a copy of
 *  * the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations under
 *  * the License.
 *
 */

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.BatchFileFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstract file batch source
 * @param <T> Type of the config
 */
public abstract class AbstractFileBatchSource<T extends FileSourceConfig>
  extends ReferenceBatchSource<Object, Object, StructuredRecord> {
  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final String INPUT_REGEX_CONFIG = "input.path.regex";
  public static final String LAST_TIME_READ = "last.time.read";
  public static final String CUTOFF_READ_TIME = "cutoff.read.time";
  public static final String USE_TIMEFILTER = "timefilter";
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_DATE_TYPE = new TypeToken<ArrayList<Date>>() { }.getType();

  private static final Logger LOG = LoggerFactory.getLogger(FileBatchSource.class);
  private final T config;
  private KeyValueTable table;
  private Date prevHour;
  private String datesToRead;
  private Path path;
  private FileSystem fs;

  public AbstractFileBatchSource(T config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    if (!config.containsMacro("timeTable") && config.timeTable != null) {
      pipelineConfigurer.createDataset(config.timeTable, KeyValueTable.class, DatasetProperties.EMPTY);
    }
    if (config.getSchema() != null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
    } else if ("text".equalsIgnoreCase(config.format)) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(PathTrackingInputFormat.getTextOutputSchema
        (config.pathField));
    } else {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }
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

    Map<String, String> properties = config.getFileSystemProperties();
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

    FileSystem pathFileSystem = FileSystem.get(new Path(config.getPath()).toUri(), conf);
    FileStatus[] fileStatus = pathFileSystem.globStatus(new Path(config.getPath()));
    fs = FileSystem.get(conf);

    if (fileStatus == null && config.ignoreNonExistingFolders) {
      LOG.warn(String.format("Input path %s does not exist and ignore non existing folder is set. " +
          "The pipeline will not read any data", config.getPath()));
      recordLineage(context);
      context.setInput(Input.of(config.referenceName,
          new SourceInputFormatProvider(EmptyInputFormat.class.getName(), conf)));
    } else if (fileStatus == null) {
      LOG.error(String.format("Input path %s does not exist and this will fail the pipeline. " +
          "To treat absence of input path as warning set Ignore Non existing folder property to true",
          config.getPath()));
      throw new RuntimeException(String.format("Input path %s does not exist", config.getPath()));
    } else {
      conf.set(INPUT_NAME_CONFIG, new Path(config.getPath()).toString());
      FileInputFormat.addInputPath(job, new Path(config.getPath()));
      if (config.maxSplitSize != null) {
        FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
      }
      if (CombinePathTrackingInputFormat.class.getName().equals(config.inputFormatClass)) {
        PathTrackingInputFormat.configure(job, conf, config.pathField, config.filenameOnly,
            config.format, config.schema, config.shouldCopyHeader());
      }
      recordLineage(context);
      context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(config.inputFormatClass, conf)));
    }
  }

  private void recordLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    Schema schema = null;
    if (config.getSchema() != null) {
      schema = config.getSchema();
    } else if ("text".equalsIgnoreCase(config.format)) {
      schema = PathTrackingInputFormat.getTextOutputSchema(config.pathField);
    }

    if (schema != null && schema.getFields() != null) {
      lineageRecorder.recordRead("Read", "Read from files",
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    // this plugin should never have (among other things) allowed specifying a custom input format class.
    // this nasty casting is here for backwards compatibility.
    if (CombinePathTrackingInputFormat.class.getName().equals(config.inputFormatClass)) {
      emitter.emit((StructuredRecord) input.getValue());
    } else {
      StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
        .set("offset", ((LongWritable) input.getKey()).get())
        .set("body", input.getValue().toString())
        .build();
      emitter.emit(output);
    }
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
}
