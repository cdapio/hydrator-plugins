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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.BatchXMLFileFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * XML Reader Batch Source Plugin
 * It is used to read XML files from HDFS with specified file properties and filters.
 * This reader emits XML event, specified by the node path property, for each file read.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("XMLReader")
@Description("Batch source for XML read from HDFS")
public class XMLReaderBatchSource extends ReferenceBatchSource<LongWritable, Object, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLReaderBatchSource.class);
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES  = new TypeToken<ArrayList<String>>() { }.getType();

  public static final Schema DEFAULT_XML_SCHEMA = Schema.recordOf(
    "xmlSchema",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING))
  );

  private final XMLReaderConfig config;

  private KeyValueTable processedFileTrackingTable;
  private FileSystem fileSystem;
  private Path tempDirectoryPath;

  public XMLReaderBatchSource(XMLReaderConfig config) {
    super(config);
    this.config = config;
  }

  @VisibleForTesting
  XMLReaderConfig getConfig() {
    return config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_XML_SCHEMA);
    if (!config.containsMacro("tableName") && !Strings.isNullOrEmpty(config.tableName)) {
      pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class.getName());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
    // Create dataset if macros were provided at configure time
    if (!Strings.isNullOrEmpty(config.tableName) && !context.datasetExists(config.tableName)) {
      context.createDataset(config.tableName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(XMLInputFormat.XML_INPUTFORMAT_PATH_NAME, config.path);
    conf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, config.nodePath);
    if (!Strings.isNullOrEmpty(config.pattern)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PATTERN, config.pattern);
    }
    conf.set(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION, config.actionAfterProcess);
    if (!Strings.isNullOrEmpty(config.targetFolder)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_TARGET_FOLDER, config.targetFolder);
    }

    if (!config.containsMacro("tableName") && !Strings.isNullOrEmpty(config.tableName)) {
      setFileTrackingInfo(context, conf);
      //Create a temporary directory, in which XMLRecordReader will add file tracking information.
      fileSystem = FileSystem.get(conf);
      long startTime = context.getLogicalStartTime();
      //Create temp file name using start time to make it unique.
      String tempDirectory = config.tableName + startTime;
      tempDirectoryPath = new Path(config.temporaryFolder, tempDirectory);
      fileSystem.mkdirs(tempDirectoryPath);
      fileSystem.deleteOnExit(tempDirectoryPath);
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FOLDER, tempDirectoryPath.toUri().toString());
    }

    XMLInputFormat.setInputPathFilter(job, BatchXMLFileFilter.class);
    XMLInputFormat.addInputPath(job, new Path(config.path));
    // create the external dataset with the given schema
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(DEFAULT_XML_SCHEMA);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(XMLInputFormat.class, conf)));
  }

  /**
   * Method to set file tracking information in to configuration.
   */
  private void setFileTrackingInfo(BatchSourceContext context, Configuration conf) {
    //For reprocessing not required, set processed file name to configuration.
    processedFileTrackingTable = context.getDataset(config.tableName);
    if (processedFileTrackingTable != null && !config.isReprocessingRequired()) {
      List<String> processedFiles = new ArrayList<String>();
      Date expiryDate = null;
      if (config.tableExpiryPeriod != null && config.tableExpiryPeriod > 0) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -Integer.valueOf(config.tableExpiryPeriod));
        expiryDate = cal.getTime();
      }

      try (CloseableIterator<KeyValue<byte[], byte[]>> iterator = processedFileTrackingTable.scan(null, null)) {
        while (iterator.hasNext()) {
          KeyValue<byte[], byte[]> keyValue = iterator.next();
          //Delete record before expiry time period
          Long time = Bytes.toLong(keyValue.getValue());
          Date processedDate = new Date(time);
          if (config.tableExpiryPeriod != null && config.tableExpiryPeriod > 0 && expiryDate != null &&
            processedDate.before(expiryDate)) {
            processedFileTrackingTable.delete(keyValue.getKey());
          } else {
            processedFiles.add(Bytes.toString(keyValue.getKey()));
          }
        }
      }
      //File name use by BatchXMLFileFilter to filter already processed files.
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_FILES,
               GSON.toJson(processedFiles, ARRAYLIST_PREPROCESSED_FILES));
    }
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    Map<String, String> xmlRecord = (Map<String, String>) input.getValue();
    Set<String> keySet = xmlRecord.keySet();
    Iterator<String>  itr = keySet.iterator();
    String fileName = Iterators.getOnlyElement(itr);
    String record = xmlRecord.get(fileName);

    StructuredRecord output = StructuredRecord.builder(DEFAULT_XML_SCHEMA)
      .set("offset", input.getKey().get())
      .set("filename", fileName)
      .set("record", record)
      .build();
    emitter.emit(output);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    if (!Strings.isNullOrEmpty(config.tableName)) {
      try {
        FileStatus[] status = fileSystem.listStatus(tempDirectoryPath);
        long processingTime = new Date().getTime();
        Path[] paths = FileUtil.stat2Paths(status);
        if (paths != null && paths.length > 0) {
          for (Path path : paths) {
            try (FSDataInputStream input = fileSystem.open(path)) {
              String key = input.readUTF();
              processedFileTrackingTable.write(Bytes.toBytes(key), Bytes.toBytes(processingTime));
            }
          }
        }
      } catch (IOException exception) {
        LOG.error("IOException occurred while reading temp directory path : " + exception.getMessage());
      }
    }
  }

  /**
   * Config class that contains all the properties needed for the XML Reader.
   */
  public static class XMLReaderConfig extends ReferencePluginConfig {
    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a \'/\'.")
    @Macro
    private final String path;

    @Nullable
    @Description("Pattern to select specific file(s)." +
      "Examples: " +
      "1. Use '^' to select files with names starting with 'catalog', such as '^catalog'. " +
      "2. Use '$' to select files with names ending with 'catalog.xml', such as 'catalog.xml$'. " +
      "3. Use '*' to select file with name contains 'catalogBook', such as 'catalogBook*'.")
    @Macro
    private final String pattern;

    @Description("Node path to emit as an individual event from the XML schema. " +
      "Example: '/book/price' to read only price under the book node")
    @Macro
    private final String nodePath;

    @Description("Action to be taken after processing of the XML file. " +
      "Possible actions are: " +
      "1. Delete from the HDFS; " +
      "2. Archived to the target location; and " +
      "3. Moved to the target location.")
    private final String actionAfterProcess;

    @Nullable
    @Description("Target folder path if user select action after process, either ARCHIVE or MOVE. " +
      "Target folder must be an existing directory.")
    @Macro
    private final String targetFolder;

    @Description("Specifies whether the file(s) should be reprocessed.")
    private final String reprocessingRequired;

    @Nullable
    @Description("Table name to be used to keep track of processed file(s).")
    @Macro
    private final String tableName;

    @Nullable
    @Description("Expiry period (days) for data in the table. Data will be persisted in the table " +
      "if no expiry period is provided." +
      "Example: For tableExpiryPeriod = 30, data before 30 days get deleted from the table.")
    @Macro
    private final Integer tableExpiryPeriod;

    @Description("An existing HDFS folder path with read and write access for the current user; required for storing " +
      "temporary files containing paths of the processed XML files. These temporary files will be read at the end of " +
      "the job to update the file track table. Default to /tmp.")
    @Macro
    private final String temporaryFolder;

    @VisibleForTesting
    XMLReaderConfig(String referenceName, String path, @Nullable String pattern, String nodePath,
                    String actionAfterProcess, @Nullable String targetFolder, String reprocessingRequired,
                    @Nullable String tableName, @Nullable Integer tableExpiryPeriod, String temporaryFolder) {
      super(referenceName);
      this.path = path;
      this.pattern = pattern;
      this.nodePath = nodePath;
      this.actionAfterProcess = actionAfterProcess;
      this.targetFolder = targetFolder;
      this.reprocessingRequired = reprocessingRequired;
      this.tableName = tableName;
      this.tableExpiryPeriod = tableExpiryPeriod;
      this.temporaryFolder = temporaryFolder;
    }

    @VisibleForTesting
    String getTableName() {
      return tableName;
    }

    boolean isReprocessingRequired() {
      return reprocessingRequired.equalsIgnoreCase("YES") ? true : false;
    }

    @VisibleForTesting
    String getPath() {
      return path;
    }

    @VisibleForTesting
    String getNodePath() {
      return nodePath;
    }

    void validate() {
      if (!containsMacro("path")) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "Path cannot be empty.");
      }
      if (!containsMacro("nodePath")) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(nodePath), "Node path cannot be empty.");
      }

      if (!containsMacro("tableExpiryPeriod") && tableExpiryPeriod != null && tableExpiryPeriod < 0) {
        throw new IllegalArgumentException("Value for Table expiry period should either be empty or greater than 0.");
      }

      if (!containsMacro("temporaryFolder")) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(temporaryFolder), "Temporary folder cannot be empty.");
      }

      boolean onlyOneActionRequired = !actionAfterProcess.equalsIgnoreCase("NONE") && isReprocessingRequired();
      Preconditions.checkArgument(!onlyOneActionRequired, "Please select either 'After Processing Action' or " +
        "'Reprocessing Required'; both cannot be applied at the same time.");

      boolean targetFolderEmpty = (actionAfterProcess.equalsIgnoreCase("ARCHIVE") ||
        actionAfterProcess.equalsIgnoreCase("MOVE")) && Strings.isNullOrEmpty(targetFolder);
      Preconditions.checkArgument(!targetFolderEmpty, "Target folder cannot be empty for Action = '" +
        actionAfterProcess + "'.");

      if (!Strings.isNullOrEmpty(pattern)) {
        try {
          Pattern.compile(pattern);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("The regular expression '%s' is not valid.", pattern), e);
        }
        // By default, the Hadoop FileInputFormat won't return any files when using a regex pattern unless the path
        // has globs in it. Checking for that scenario.
        if (path.endsWith("/") ||
          !(path.contains("*") || path.contains("?") || path.contains("{") || path.contains("["))) {
          throw new IllegalArgumentException("When filtering with regular expressions, " +
                                               "the path must be a directory and leverage glob syntax. Usually " +
                                               "the folder path needs to end with '/*'.");
        }
      }
    }
  }
}
