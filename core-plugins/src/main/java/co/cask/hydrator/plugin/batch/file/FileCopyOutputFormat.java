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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class for the OutputFormat that FileCopySink uses.
 */
public class FileCopyOutputFormat extends OutputFormat {
  public static final String BASE_PATH = "base.path";
  public static final String ENABLE_OVERWRITE = "enable.overwrite";
  public static final String PRESERVE_OWNER = "preserve.owner";
  public static final String BUFFER_SIZE = "buffer.size";
  public static final String FS_HOST_URI = "filesystem.host.uri";

  private static final Logger LOG = LoggerFactory.getLogger(FileCopyOutputFormat.class);

  public static void setBasePath(Map<String, String> conf, String value) {
    conf.put(BASE_PATH, value);
  }

  public static void setEnableOverwrite(Map<String, String> conf, String value) {
    conf.put(ENABLE_OVERWRITE, value);
  }

  public static void setPreserveFileOwner(Map<String, String> conf, String value) {
    conf.put(PRESERVE_OWNER, value);
  }

  public static void setBufferSize(Map<String, String> conf, String value) {
    conf.put(BUFFER_SIZE, value);
  }

  public static void setFilesystemHostUri(Map<String, String> conf, String value) {
    conf.put(FS_HOST_URI, value);
  }


  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    // check if base path is set
    if (jobContext.getConfiguration().get(BASE_PATH, null) == null) {
      throw new IOException("Base path not set.");
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    // TODO: implement an OutputCommitter for file copying jobs, or investigate whether we can use FileOutputCommitter
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        // no op
      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
        // no op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
        // no op
      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
        // no op
      }
    };
  }

  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    return new FileCopyRecordWriter(conf);
  }
}
