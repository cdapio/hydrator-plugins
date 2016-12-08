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

package co.cask.hydrator.plugin.common;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
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
import java.nio.ByteBuffer;

/**
 *
 */
public class KinesisOutputFormat extends OutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisOutputFormat.class);
  private static final int MAX_RETRIES = 3;
  private static AmazonKinesisClient kinesisClient;

  /**
   * Get the {@link RecordWriter} for the given task.
   *
   * @param context the information about the current task.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new KinesisRecordWriter(kinesisClient, context);
  }

  /**
   * Check for validity of the output-specification for the job and initialize {@link AmazonKinesisClient}
   * This is to validate the output specification for the job when it is a job is submitted.
   *
   * @param context information about the job
   * @throws IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    BasicAWSCredentials awsCred = new BasicAWSCredentials(conf.get(Properties.KinesisRealtimeSink.ACCESS_ID),
                                                          conf.get(Properties.KinesisRealtimeSink.ACCESS_KEY));

    if (kinesisClient == null) {
      LOG.info("Creating amazon kinesis client for stream {}", Properties.KinesisRealtimeSink.NAME);
      kinesisClient = new AmazonKinesisClient(awsCred);
      KinesisUtil.createAndWaitForStream(kinesisClient, conf.get(Properties.KinesisRealtimeSink.NAME),
                                         Integer.valueOf(conf.get(Properties.KinesisRealtimeSink.SHARD_COUNT)));
    }
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

  private class KinesisRecordWriter extends RecordWriter<NullWritable, Text> {
    private final AmazonKinesisClient kinesisClient;
    private final TaskAttemptContext context;
    private final boolean distribute;
    private final String streamName;

    KinesisRecordWriter(AmazonKinesisClient kinesisClient, TaskAttemptContext context) {
      this.kinesisClient = kinesisClient;
      this.context = context;
      this.distribute = context.getConfiguration().getBoolean(Properties.KinesisRealtimeSink.DISTRIBUTE, true);
      this.streamName = context.getConfiguration().get(Properties.KinesisRealtimeSink.NAME);
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
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(streamName);
      // uniformly distribute data between shards if distribute is set to true, Otherwise write everything to a
      // single shard
      String dist = distribute ? String.valueOf(Math.random()) : String.valueOf(distribute);
      putRecordRequest.setPartitionKey(dist);
      ByteBuffer buffer = ByteBuffer.wrap(value.toString().getBytes());
      sendKinesisRecord(buffer, putRecordRequest);
    }

    private void sendKinesisRecord(ByteBuffer data, PutRecordRequest putRecordRequest) {
      for (int i = 0; i < MAX_RETRIES; i++) {
        putRecordRequest.setData(data);
        try {
          kinesisClient.putRecord(putRecordRequest);
          return;
        } catch (ProvisionedThroughputExceededException ex) {
          LOG.debug("Throughput exceeded. Sleeping for 10 ms and retrying.");
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            LOG.info("Retry wait interrupted", e);
          }
        }
      }
      LOG.error("Maximum retries exhausted");
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
