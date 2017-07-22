/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class for the OutputFormat that FileCopySink uses.
 */
public class FileCopyOutputFormat extends OutputFormat {
  protected static final String BASE_PATH = "base.path";
  protected static final String ENABLE_OVERWRITE = "enable.overwrite";
  protected static final String BUFFER_SIZE = "buffer.size";

  private static final Logger LOG = LoggerFactory.getLogger(FileCopyOutputFormat.class);

  public static void setBasePath(Map<String, String> conf, String value) {
    conf.put(BASE_PATH, value);
  }

  public static void setEnableOverwrite(Map<String, String> conf, String value) {
    conf.put(ENABLE_OVERWRITE, value);
  }

  public static void setBufferSize(Map<String, String> conf, String value) {
    conf.put(BUFFER_SIZE, value);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    try {
      return new FileOutputCommitter(new Path(taskAttemptContext.getConfiguration().get(BASE_PATH)),
                                     taskAttemptContext);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      return new FileCopyRecordWriter(conf);
    } catch (Exception e) {
      return null;
    }
  }
}
