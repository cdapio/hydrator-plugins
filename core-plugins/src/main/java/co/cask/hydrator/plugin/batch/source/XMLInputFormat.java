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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * InputFormat class for XMLReader plugin.
 */
public class XMLInputFormat extends FileInputFormat<LongWritable, Map<String, String>> {
  public static final String XML_INPUTFORMAT_PATH_NAME = "xml.inputformat.path.name";
  public static final String XML_INPUTFORMAT_NODE_PATH = "xml.inputformat.node.path";
  public static final String XML_INPUTFORMAT_PATTERN = "xml.inputformat.pattern";
  public static final String XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FOLDER = "xml.inputformat.processed.data.temp.folder";
  public static final String XML_INPUTFORMAT_PROCESSED_FILES = "xml.inputformat.processed.files";
  public static final String XML_INPUTFORMAT_FILE_ACTION = "xml.inputformat.file.action";
  public static final String XML_INPUTFORMAT_TARGET_FOLDER = "xml.inputformat.target.folder";

  @Override
  public RecordReader<LongWritable, Map<String, String>> createRecordReader(InputSplit split,
                                                                            TaskAttemptContext context)
    throws IOException {
    return new XMLRecordReader();
  }

  protected boolean isSplitable(JobContext context, Path file) {
    //XML files are not splittable hence returning false.
    return false;
  }
}
