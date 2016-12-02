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

package co.cask.hydrator.plugin.batch.sink;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class KuduOutputFormat extends OutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

  /**
   * Get the {@link RecordWriter} for the given task.
   *
   * @param context the information about the current task.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new KuduRecordWriter(context);
  }

  /**
   * Check for validity of the output-specification for the job and initialize {@link AmazonKinesisClient}
   * <p>
   * <p>This is to validate the output specification for the job when it is
   * a job is submitted.</p>
   *
   * @param context information about the job
   * @throws IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
  }

  /**
   * Get the output committer for this output format. This is responsible
   * for ensuring the output is committed correctly.
   *
   * @param context the task context
   * @return an output committer
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new NoOpOutputCommitter();
  }

  private class KuduRecordWriter extends RecordWriter<NullWritable, Text> {
    private final TaskAttemptContext context;

    private KuduRecordWriter(TaskAttemptContext context) {
      this.context = context;
    }


    /**
     * Writes a key/value pair.
     *
     * @param key   the key to write.
     * @param value the value to write.
     * @throws IOException
     */
    @Override
    public void write(NullWritable key, Text value) throws IOException, InterruptedException {
    }

    /**
     * @param context the context of the task
     * @throws IOException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      //No operation here
    }
  }
}
