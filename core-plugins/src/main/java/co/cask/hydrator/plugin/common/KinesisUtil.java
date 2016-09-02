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

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Utility methods to help create and manage Kinesis Streams
 */
public final class KinesisUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisUtil.class);
  private static final long TIMEOUT = 3 * 60 * 1000;
  private static final long SLEEP_INTERVAL = 1000 * 10;

  private KinesisUtil() {
  }

  /**
   * Creates an Amazon Kinesis stream if it does not exist and waits for it to become available
   *
   * @param kinesisClient The {@link AmazonKinesisClient} with Amazon Kinesis read and write privileges
   * @param streamName The Amazon Kinesis stream name to create
   * @param shardCount The shard count to create the stream with
   * @throws IllegalStateException Invalid Amazon Kinesis stream state
   * @throws IllegalStateException Stream does not become active before the timeout
   */
  public static void createAndWaitForStream(AmazonKinesisClient kinesisClient, String streamName, int shardCount) {
    StreamStatus streamStatus = getStreamState(kinesisClient, streamName);

    if (streamStatus != null) {
      if (streamStatus == StreamStatus.ACTIVE) {
        return;
      }
      if (streamStatus == StreamStatus.DELETING) {
        waitForStreamState(kinesisClient, streamName, null);
        createStream(streamName, shardCount, kinesisClient);
      }
    } else {
      createStream(streamName, shardCount, kinesisClient);
    }
    waitForStreamState(kinesisClient, streamName, StreamStatus.ACTIVE);
  }

  /**
   * waits for the stream to be in the expected state
   *
   * @param kinesisClient
   * @param streamName
   * @param expectedStatus
   * @throws IllegalStateException
   */
  private static void waitForStreamState(AmazonKinesisClient kinesisClient, String streamName,
                                         @Nullable StreamStatus expectedStatus) {
    StreamStatus streamStatus = getStreamState(kinesisClient, streamName);
    long waitTime = System.currentTimeMillis() + TIMEOUT;
    while (streamStatus != expectedStatus && waitTime > System.currentTimeMillis()) {
      Uninterruptibles.sleepUninterruptibly(SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
      streamStatus = getStreamState(kinesisClient, streamName);
    }
    if (!(streamStatus == expectedStatus)) {
      throw new IllegalStateException(String.format("Timed out waiting for stream to be in %s state",
                                                    expectedStatus));
    }
  }

    private static void createStream(String streamName, int shardCount, AmazonKinesisClient kinesisClient) {
      CreateStreamRequest createStreamRequest = new CreateStreamRequest();
      createStreamRequest.setStreamName(streamName);
      createStreamRequest.setShardCount(shardCount);
      kinesisClient.createStream(createStreamRequest);
      LOG.info("Stream {} is being created", streamName);
    }

    /**
     * Return the state of a Amazon Kinesis stream.
     *
     * @param kinesisClient The {@link AmazonKinesisClient} with Amazon Kinesis read privileges
     * @param streamName The Amazon Kinesis stream to get the state of
     * @return String representation of the Stream state
     */
  private static StreamStatus getStreamState(AmazonKinesisClient kinesisClient, String streamName) {
    try {
      DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
      describeStreamRequest.setStreamName(streamName);
      String status = kinesisClient.describeStream(describeStreamRequest).getStreamDescription().getStreamStatus();
      return StreamStatus.fromValue(status);
    } catch (ResourceNotFoundException e) {
      return null;
    }
  }
}
