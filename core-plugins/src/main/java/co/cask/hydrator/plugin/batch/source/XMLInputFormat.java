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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * InputFormat class for XMLReader plugin.
 */
public class XMLInputFormat extends FileInputFormat<LongWritable, Map<String, String>> {
  public static final String XML_INPUTFORMAT_NODE_PATH = "xml.inputformat.node.path";
  public static final String XML_INPUTFORMAT_PATTERN = "xml.inputformat.pattern";
  public static final String XML_INPUTFORMAT_REPROCESSING_REQUIRED = "xml.inputformat.reprocessing.required";
  public static final String XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FILE = "xml.inputformat.processed.data.temp.file";
  public static final String XML_INPUTFORMAT_FILE_ACTION = "xml.inputformat.file.action";
  public static final String XML_INPUTFORMAT_TARGET_FOLDER = "xml.inputformat.target.folder";

  @Override
  public RecordReader<LongWritable, Map<String, String>> createRecordReader(InputSplit split,
                                                                            TaskAttemptContext context)
    throws IOException {
    return new XMLRecordReader((FileSplit) split, context.getConfiguration());
  }

  protected boolean isSplitable(JobContext context, Path file) {
    //XML files are not splittable hence returning false.
    return false;
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> listStatus = super.listStatus(job);
    if (listStatus != null && !listStatus.isEmpty()) {
      //apply pattern on the file list status.
      String pattern = job.getConfiguration().get(XML_INPUTFORMAT_PATTERN);
      if (StringUtils.isNotEmpty(pattern)) {
        listStatus = getPatternMatchedListStatus(pattern, listStatus);
      }
      if (CollectionUtils.isNotEmpty(listStatus)) {
        listStatus = getPreProcessedFilteredListStatus(job.getConfiguration(), listStatus);
      }
    }
    return listStatus;
  }

  /**
   * Method to filter files from the preprocessed file list.
   * @param conf
   * @param listStatusList
   * @return List<FileStatus> - filtered file list.
   */
  private List<FileStatus> getPreProcessedFilteredListStatus(Configuration conf, List<FileStatus> listStatusList) {
    String processingReq = conf.get(XML_INPUTFORMAT_REPROCESSING_REQUIRED);
    String processedDataTempFile = conf.get(XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FILE);
    //filter files only if reprocessing not required.
    if (processingReq.equalsIgnoreCase("NO") && StringUtils.isNotEmpty(processedDataTempFile)) {
      List<FileStatus> includedFileListStatus = new ArrayList<FileStatus>();
      List<String> processedFileList = getPreProcessedFileList(processedDataTempFile);

      for (FileStatus listStatus : listStatusList) {
        String filePath = listStatus.getPath().toString();
        if (!processedFileList.contains(filePath)) {
          includedFileListStatus.add(listStatus);
        }
      }
      return includedFileListStatus;
    }
    return listStatusList;
  }

  /**
   * Method to read file tracking information using temporary file.
   * This temporary file is in sync with file tracking information dataset.
   * @param processedDataTempFile
   * @return List<String> - List of pre-processed files.
   */
  private List<String> getPreProcessedFileList(String processedDataTempFile) {
    List<String> processedFileList = new ArrayList<String>();
    try {
      File file = new File(processedDataTempFile);
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String line = null;
      while ((line = reader.readLine()) != null) {
        processedFileList.add(line.trim());
      }
      reader.close();
      //delete file contents so that only new processed file information can be added.
      PrintWriter writer = new PrintWriter(file);
      writer.print("");
      writer.close();
    } catch (IOException exception) {
      throw new IllegalArgumentException("Error while reading pre-processed tracking information file : "
                                           + exception.getMessage());
    }
    return processedFileList;
  }

  private List<FileStatus> getPatternMatchedListStatus(String pattern, List<FileStatus> listStatus) {
    List<FileStatus> matchedFileListStatus = new ArrayList<FileStatus>();
    for (FileStatus status : listStatus) {
      String filename = status.getPath().getName();
      Pattern regex = Pattern.compile(pattern);
      Matcher matcher = regex.matcher(filename);
      if (matcher.find()) {
        matchedFileListStatus.add(status);
      }
    }
    return matchedFileListStatus;
  }
}
