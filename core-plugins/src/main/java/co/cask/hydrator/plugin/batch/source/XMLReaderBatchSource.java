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
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.BatchXMLFileFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * XML Reader Batch Source Plugin
 * It is used to read XML files from HDFS with specified file properties and filters.
 * It parses the read file into specified Output Schema.
 * A {@link FileBatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("XMLReader")
@Description("Batch source for XML read from HDFS")
public class XMLReaderBatchSource extends ReferenceBatchSource<LongWritable, Object, StructuredRecord> {
  protected static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'.";
  protected static final String PATTERN_DESCRIPTION = "Pattern to select specific file(s)." +
    "Example - " +
    "1. Use '^' to select file with name start with 'catalog', like '^catalog'." +
    "2. Use '$' to select file with name end with 'catalog.xml', like 'catalog.xml$'." +
    "3. Use '*' to select file with name contains 'catalogBook', like 'catalogBook*'.";
  protected static final String NODE_PATH_DESCRIPTION = "Node path to emit individual event from the schema. " +
    "Example - '/book/price' to read only price under the book node";
  protected static final String AFTER_PROCESS_ACTION_DESCRIPTION = "Action to be taken after processing of the XML " +
    "file. " +
    "Possible actions are - " +
    "1. Delete from the HDFS." +
    "2. Archived to the target location." +
    "3. Moved to the target location.";
  protected static final String TARGET_FOLDER_DESCRIPTION = "Target folder path if user select action after process, " +
    "either ARCHIVE or MOVE";
  protected static final String FILES_TO_REPROCESS_DESCRIPTION = "Specifies whether the file(s) should be " +
    "preprocessed.";
  protected static final String TABLE_NAME_DESCRIPTION = "Name of the table to keep track of processed file(s).";

  public static final Schema DEFAULT_XML_SCHEMA = Schema.recordOf(
    "xmlSchema",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING))
  );

  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES  = new TypeToken<ArrayList<String>>() { }.getType();

  private KeyValueTable processedFileTrackingTable;
  private final XMLReaderConfig config;

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
    config.validateConfig();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_XML_SCHEMA);
    if (StringUtils.isNotEmpty(config.tableName)) {
      pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class.getName());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    if (StringUtils.isNotEmpty(config.pattern)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_PATTERN, config.pattern);
    }
    XMLInputFormat.addInputPaths(job, config.path);
    conf.set(XMLInputFormat.XML_INPUTFORMAT_REPROCESSING_REQUIRED, this.config.reprocessingReq);
    //set file tracking information in a temporary file, to be available to read outside plugin.
    setFileTrackingInfo(context, conf);
    if (StringUtils.isNotEmpty(this.config.actionAfterProcess)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION, this.config.actionAfterProcess);
    }
    if (StringUtils.isNotEmpty(this.config.actionAfterProcess)) {
      conf.set(XMLInputFormat.XML_INPUTFORMAT_TARGET_FOLDER, this.config.targetFolder);
    }
    conf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, this.config.nodePath);
    XMLInputFormat.setInputPathFilter(job, BatchXMLFileFilter.class);
    conf.set(XMLInputFormat.XML_INPUT_NAME_CONFIG, config.path);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(XMLInputFormat.class, conf)));
  }

  /**
   * Method to set file tracking information.
   * @param context
   * @param conf
   * @throws IOException
   */
  private void setFileTrackingInfo(BatchSourceContext context, Configuration conf) throws IOException {
    String tableName = this.config.tableName;
    if (StringUtils.isNotEmpty(tableName)) {
      try {
        processedFileTrackingTable = context.getDataset(tableName);
        if (processedFileTrackingTable != null) {
          List<String> processedFiles =  new ArrayList<String>();
          File tempFile = new File(tableName);
          // if file exists, then delete it and create new.
          if (tempFile.exists()) {
            tempFile.delete();
          }
          tempFile.createNewFile();
          CloseableIterator<KeyValue<byte[], byte[]>> iterator = processedFileTrackingTable.scan(null, null);

          //write file tracking information to temporary file.
          while (iterator.hasNext()) {
            KeyValue<byte[], byte[]> keyValue = iterator.next();
            processedFiles.add(new String(keyValue.getKey(), Charsets.UTF_8));
          }
          conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FILE, tableName);
          if (config.reprocessingReq.equalsIgnoreCase("NO")) {
            conf.set(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_FILES,
                     GSON.toJson(processedFiles, ARRAYLIST_PREPROCESSED_FILES));
          }
        }
      } catch (DatasetInstantiationException datasetInstantiationException) {
        throw Throwables.propagate(datasetInstantiationException);
      }
    }
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    Map<String, String> xmlRecord = (Map<String, String>) input.getValue();
    Set<String> keySet = xmlRecord.keySet();
    Iterator<String>  itr = keySet.iterator();
    String fileName = itr.next();
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
    if (StringUtils.isNotEmpty(this.config.tableName)) {
      try {
        //read file tracking information updated by XMLRecordReader and put it in dataset.
        File file = new File(this.config.tableName);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
          long processingTime = new Date().getTime();
          processedFileTrackingTable.write(line.getBytes(Charsets.UTF_8),
                                           String.valueOf(processingTime).getBytes(Charsets.UTF_8));
        }
        reader.close();
        file.delete();
      } catch (IOException exception) {
        exception.printStackTrace();
      }
    }
  }

  /**
   * Config class that contains all the properties needed for the XML Reader.
   */
  public static class XMLReaderConfig extends ReferencePluginConfig {
    @Description(PATH_DESCRIPTION)
    public String path;

    @Nullable
    @Description(PATTERN_DESCRIPTION)
    public String pattern;

    @Description(NODE_PATH_DESCRIPTION)
    public String nodePath;

    @Nullable
    @Description(AFTER_PROCESS_ACTION_DESCRIPTION)
    public String actionAfterProcess;

    @Nullable
    @Description(TARGET_FOLDER_DESCRIPTION)
    public String targetFolder;

    @Description(FILES_TO_REPROCESS_DESCRIPTION)
    public String reprocessingReq;

    @Description(TABLE_NAME_DESCRIPTION)
    public String tableName;

    @VisibleForTesting
    public XMLReaderConfig(String referenceName, String path, @Nullable String pattern,
                           @Nullable String nodePath, @Nullable String actionAfterProcess,
                           @Nullable String targetFolder, String reprocessingReq, @Nullable String tableName) {
      super(referenceName);
      this.path = path;
      this.pattern = pattern;
      this.nodePath = nodePath;
      this.actionAfterProcess = actionAfterProcess;
      this.targetFolder = targetFolder;
      this.reprocessingReq = reprocessingReq;
      this.tableName = tableName;
    }

    @VisibleForTesting
    public String getTableName() {
      return this.tableName;
    }

    @VisibleForTesting
    public String getReprocessingReq() {
      return this.reprocessingReq;
    }

    @VisibleForTesting
    public String getPath() {
      return this.path;
    }

    @VisibleForTesting
    public String getNodePath() {
      return this.nodePath;
    }

    public void validateConfig() {
      if (Strings.isNullOrEmpty(this.path)) {
        throw new IllegalArgumentException("Path cannot be empty.");
      } else if (Strings.isNullOrEmpty(this.nodePath)) {
        throw new IllegalArgumentException("Node path cannot be empty.");
      } else if (Strings.isNullOrEmpty(this.tableName)) {
        throw new IllegalArgumentException("Table Name cannot be empty.");
      } else if (!this.actionAfterProcess.equalsIgnoreCase("NONE") && this.reprocessingReq.equalsIgnoreCase("YES")) {
        throw new IllegalArgumentException("Please select either 'After Processing Action' or 'Reprocessing " +
                                             "Required', both cannot be applied at same time.");
      } else if ((this.actionAfterProcess.equalsIgnoreCase("ARCHIVE") ||
        this.actionAfterProcess.equalsIgnoreCase("MOVE")) && Strings.isNullOrEmpty(this.targetFolder)) {
        throw new IllegalArgumentException("Target folder cannot be Empty for Action = " + actionAfterProcess + ".");
      }
    }
  }
}
