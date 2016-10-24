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
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Spark streaming source to get data from AWS Kinesis streams
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("KinesisSource")
@Description("Kinesis streaming source.")
public class KinesisStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamingSource.class);
  private final KinesisStreamConfig config;

  public KinesisStreamingSource(KinesisStreamConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.streamName),
                                "Stream name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessKeyId),
                                "Access Key should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessSecret),
                                "Access Key secret should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.region),
                                "Region name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.appName),
                                "Application name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.endpoint),
                                "Endpoint url should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.schema),
                                "Schema should be non-null, non-empty.");
    config.validate();
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    registerUsage(streamingContext);
    BasicAWSCredentials awsCred = new BasicAWSCredentials(config.awsAccessKeyId, config.awsAccessSecret);
    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(awsCred);
    JavaStreamingContext javaStreamingContext = streamingContext.getSparkStreamingContext();
    Duration kinesisCheckpointInterval = new Duration(config.duration);

    int numShards = kinesisClient.describeStream(config.streamName).getStreamDescription().getShards().size();
    List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numShards);
    LOG.info("creating {} spark executors for {} shards", numShards, numShards);
    //Creating spark executors based on the number of shards in the stream
    for (int i = 0; i < numShards; i++) {
      streamsList.add(
        KinesisUtils.createStream(javaStreamingContext, config.appName, config.streamName, config.endpoint,
                                  config.region, config.getInitialPosition(), kinesisCheckpointInterval,
                                  StorageLevel.MEMORY_AND_DISK_2(), config.awsAccessKeyId, config.awsAccessSecret)
      );
    }

    // Union all the streams if there is more than 1 stream
    JavaDStream<byte[]> kinesisStream;
    if (streamsList.size() > 1) {
      kinesisStream = javaStreamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
    } else {
      // Otherwise, just use the 1 stream
      kinesisStream = streamsList.get(0);
    }
    return kinesisStream.map(config.getFormat() == null ? new BytesFunction(config) : new FormatFunction(config));
  }

  /**
   * Transforms Kinesis payload into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   * Output schema must contain only 1 field of type bytes
   */
  private static class BytesFunction implements Function<byte[] , StructuredRecord> {
    private final KinesisStreamConfig config;
    private transient String messageField;
    private transient Schema schema;

    BytesFunction(KinesisStreamConfig config) {
      this.config = config;
    }

    @Override
    public StructuredRecord call(byte[] data) throws Exception {
      // first time this was called, initialize schema
      if (schema == null) {
        schema = config.parseSchema();
        StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
        messageField = schema.getFields().get(0).getName();
        recordBuilder.set(messageField, data);
        return recordBuilder.build();
      }
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      recordBuilder.set(messageField, data);
      return recordBuilder.build();
    }
  }

  /**
   * Transforms Kinesis payload into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction implements Function<byte[] , StructuredRecord> {
    private final KinesisStreamConfig config;
    private transient Schema outputSchema;
    private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

    FormatFunction(KinesisStreamConfig config) {
      this.config = config;
    }

    @Override
    public StructuredRecord call(byte[] data) throws Exception {
      // first time this was called, initialize schema
      if (recordFormat == null) {
        outputSchema = config.parseSchema();
        Schema messageSchema = config.parseSchema();
        FormatSpecification spec =
          new FormatSpecification(config.getFormat(), messageSchema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(data)));
      for (Schema.Field messageField : messageRecord.getSchema().getFields()) {
        String fieldName = messageField.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
      return builder.build();
    }
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisStreamConfig extends ReferencePluginConfig implements Serializable {

    @Name("appName")
    @Description(" The application name that will be used to checkpoint the Kinesis sequence numbers in DynamoDB table")
    @Macro
    private String appName;

    @Name("streamName")
    @Description("The name of the Kinesis stream to the get the data from. The stream should be active")
    @Macro
    private String streamName;

    @Name("endpointUrl")
    @Description("Valid Kinesis endpoints URL eg. kinesis.us-east-1.amazonaws.com")
    @Macro
    private String endpoint;

    @Name("region")
    @Description("AWS region specific to the stream")
    @Macro
    private String region;

    @Name("duration")
    @Description("The interval (e.g., Duration(2000) = 2 seconds) at which the Kinesis Client Library saves its " +
      "position in the stream.")
    @Macro
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

    @Description("Optional format of the Kinesis stream shard. Any format supported by CDAP is supported. For " +
      "example, a value of 'csv' will attempt to parse Kinesis payloads as comma-separated values. If no format is " +
      "given, Kinesis shard payloads will be treated as bytes.")
    @Nullable
    private String format;

    @Description("Output schema of the source, The fields are used in conjunction with the format to parse Kinesis " +
      "payloads")
    private String schema;

    public KinesisStreamConfig(String referenceName, String appName, String streamName, String endpoint, String region,
                               Integer duration, String initialPosition, String awsAccessKeyId, String awsAccessSecret,
                               String format, String schema) {
      super(referenceName);
      this.appName = appName;
      this.streamName = streamName;
      this.endpoint = endpoint;
      this.region = region;
      this.duration = duration;
      this.initialPosition = initialPosition;
      this.awsAccessKeyId = awsAccessKeyId;
      this.awsAccessSecret = awsAccessSecret;
      this.format = format;
      this.schema = schema;
    }

    @Nullable
    public String getFormat() {
      return Strings.isNullOrEmpty(format) ? null : format;
    }

    public Schema parseSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException | NullPointerException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }

    public void validate() {
      Schema messageSchema = parseSchema();
      // if format is empty, there must be just a single message field of type bytes or nullable types.
      if (Strings.isNullOrEmpty(format)) {
        List<Schema.Field> messageFields = messageSchema.getFields();
        if (messageFields.size() > 1) {
          List<String> fieldNames = new ArrayList<>();
          for (Schema.Field messageField : messageFields) {
            fieldNames.add(messageField.getName());
          }
          throw new IllegalArgumentException(String.format(
            "Without a format, the schema must contain just a single message field of type bytes or nullable bytes. " +
              "Found %s message fields (%s).", messageFields.size(), Joiner.on(',').join(fieldNames)));
        }
        Schema.Field messageField = messageFields.get(0);
        Schema messageFieldSchema = messageField.getSchema();
        Schema.Type messageFieldType = messageFieldSchema.isNullable() ?
          messageFieldSchema.getNonNullable().getType() : messageFieldSchema.getType();
        if (messageFieldType != Schema.Type.BYTES) {
          throw new IllegalArgumentException(String.format(
            "Without a format, the message field must be of type bytes or nullable bytes, but field %s is of type %s.",
            messageField.getName(), messageField.getSchema()));
        }
      } else {
        // otherwise, if there is a format, make sure we can instantiate it.
        FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<String, String>());

        try {
          RecordFormats.createInitializedFormat(formatSpec);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format(
            "Unable to instantiate a message parser from format '%s' and message schema '%s': %s",
            format, messageSchema, e.getMessage()), e);
        }
      }
    }

    private InitialPositionInStream getInitialPosition() {
      if (initialPosition.toUpperCase().equals(InitialPositionInStream.TRIM_HORIZON.name())) {
        return InitialPositionInStream.TRIM_HORIZON;
      } else {
        return InitialPositionInStream.LATEST;
      }
    }
  }
}
