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

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Spark streaming source to get data from AWS Kinesis streams
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("KinesisSpark")
@Description("Kinesis streaming source.")
public class KinesisStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamingSource.class);
  private static final Schema SCHEMA = Schema.recordOf("kinesis",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private final KinesisStreamConfig config;

  public KinesisStreamingSource(KinesisStreamConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    registerUsage(streamingContext);
    JavaStreamingContext javaStreamingContext = streamingContext.getSparkStreamingContext();
    Duration kinesisCheckpointInterval = new Duration(config.duration);
    JavaReceiverInputDStream<byte[]> kinesisStream = KinesisUtils.createStream(javaStreamingContext, config.appName,
                                                                               config.streamName, config.endpoint,
                                                                               config.getRegionName(),
                                                                               config.getInitialPosition(),
                                                                               kinesisCheckpointInterval,
                                                                               StorageLevel.MEMORY_AND_DISK_2(),
                                                                               config.awsAccessKeyId,
                                                                               config.awsAccessSecret);

    return kinesisStream.map(new Function<byte[], StructuredRecord>() {
                               public StructuredRecord call(byte[] data) {
                                 return convertText(data);
                               }
                             }
    );
  }

  private StructuredRecord convertText(byte[] data) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(SCHEMA);
    recordBuilder.set("body", new String(data));
    return recordBuilder.build();
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisStreamConfig extends ReferencePluginConfig implements Serializable {

    @Name("appName")
    @Description(" The application name that will be used to checkpoint the Kinesis sequence numbers in DynamoDB table")
    private String appName;

    @Name("streamName")
    @Description("The name of the Kinesis stream to output to. The stream should be active")
    @Macro
    private String streamName;

    @Name("endpointUrl")
    @Description("Valid Kinesis endpoints URL")
    private String endpoint;

    @Name("duration")
    @Description("The interval (e.g., Duration(2000) = 2 seconds) at which the Kinesis Client Library saves its " +
      "position in the stream.")
    private Integer duration;

    @Name("initialPosition")
    @Description("Can be either TRIM_HORIZON or LATEST, Default position will be Latest")
    private String initialPosition;

    @Name("awsAccessKeyId")
    @Description("AWS access Id having access to Kinesis streams")
    @Macro
    private String awsAccessKeyId;

    @Name("awsAccessSecret")
    @Description("AWS access key secret having access to Kinesis streams")
    @Macro
    private String awsAccessSecret;

    public KinesisStreamConfig(String referenceName, String appName, String streamName, String endpoint,
                               Integer duration, String initialPosition, String awsAccessKeyId,
                               String awsAccessSecret) {
      super(referenceName);
      this.appName = appName;
      this.streamName = streamName;
      this.endpoint = endpoint;
      this.duration = duration;
      this.initialPosition = initialPosition;
      this.awsAccessKeyId = awsAccessKeyId;
      this.awsAccessSecret = awsAccessSecret;
    }

    public String getRegionName() {
      return RegionUtils.getRegionByEndpoint(endpoint).getName();
    }

    public InitialPositionInStream getInitialPosition() {
      if (initialPosition.toUpperCase().equals("TRIM_HORIZON")) {
        return InitialPositionInStream.TRIM_HORIZON;
      } else {
        return InitialPositionInStream.LATEST;
      }
    }
  }
}
