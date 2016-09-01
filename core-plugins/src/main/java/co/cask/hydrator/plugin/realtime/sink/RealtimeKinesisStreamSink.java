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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.hydrator.plugin.common.KinesisUtil;
import co.cask.hydrator.plugin.common.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;


/**
 * A {@link RealtimeSink} that writes data to a Amazon Kinesis stream.
 * If Kinesis Stream does not exist, it will be created using properties provided with this sink.
 */
@Plugin(type = RealtimeSink.PLUGIN_TYPE)
@Name("KinesisSink")
@Description("Real-time sink that outputs to a specified AWS Kinesis stream.")
public class RealtimeKinesisStreamSink extends RealtimeSink<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(RealtimeKinesisStreamSink.class);
  private final KinesisConfig config;
  private AmazonKinesisClient kinesisClient;

  public RealtimeKinesisStreamSink(KinesisConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.name),
                                "Stream name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessKey),
                                "Access Key should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.awsAccessSecret),
                                "Access Key secret should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.partition),
                                "Partition name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.bodyField),
                                "Input field should be non-null, non-empty.");
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    validateInputSchema(inputSchema);
  }

  private void validateInputSchema(@Nullable Schema inputSchema) {
    if (inputSchema == null) {
      return;
    }
    // Check the existence of field in input schema
    Schema.Field inputSchemaField = inputSchema.getField(config.bodyField);
    if (inputSchemaField == null) {
      throw new IllegalArgumentException(
        "Field " + config.bodyField + " is not present in the input schema");
    }

    // Check that the field is of primitive type
    Schema fieldSchema = inputSchemaField.getSchema();
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (!fieldType.isSimpleType()) {
      throw new IllegalArgumentException(String.format(
        "Type %s for field %s is not supported. Supported types are %s, %s, %s, %s, %s, %s, %s", Schema.Type.BOOLEAN,
        Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING, Schema.Type.BYTES,
        fieldType, config.bodyField));
    }
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    BasicAWSCredentials awsCred = new BasicAWSCredentials(config.awsAccessKey, config.awsAccessSecret);
    kinesisClient = new AmazonKinesisClient(awsCred);
    KinesisUtil.createAndWaitForStream(kinesisClient, config.name, config.shardCount);
  }

  @Override
  public void destroy() {
    kinesisClient.shutdown();
  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;
    for (StructuredRecord structuredRecord : structuredRecords) {
      Schema schema = structuredRecord.getSchema();
      Object data = structuredRecord.get(config.bodyField);
      if (data == null) {
        LOG.debug("Found null data. Skipping record.");
        continue;
      }
      Schema.Field dataSchemaField = schema.getField(config.bodyField);
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(config.name);
      putRecordRequest.setPartitionKey(config.partition);
      switch (dataSchemaField.getSchema().getType()) {
        case BYTES:
          numRecordsWritten += writeBytes(putRecordRequest, data);
          break;
        case STRING:
          numRecordsWritten += writeString(putRecordRequest, data);
          break;
        case LONG:
        case DOUBLE:
        case FLOAT:
        case BOOLEAN:
        case INT:
          numRecordsWritten += writeString(putRecordRequest, String.valueOf(data));
          break;
        default:
          throw new UnsupportedTypeException(String.format("Type %s is not supported for writing to stream",
                                                           data.getClass().getName()));
      }
    }
    return numRecordsWritten;
  }

  private int writeString(PutRecordRequest putRecordRequest, Object data) {
    putRecordRequest.setData(ByteBuffer.wrap(Bytes.toBytes((String) data)));
    return putRecord(putRecordRequest);
  }

  private int writeBytes(PutRecordRequest putRecordRequest, Object data) {
    ByteBuffer buffer;
    if (data instanceof ByteBuffer) {
      buffer = (ByteBuffer) data;
    } else if (data instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) data);
    } else {
      throw new IllegalStateException(String.format("Type %s is not supported for writing to stream",
                                                    data.getClass().getName()));
    }
    putRecordRequest.setData(buffer);
    return putRecord(putRecordRequest);
  }

  private int putRecord(PutRecordRequest putRecordRequest) {
    try {
      kinesisClient.putRecord(putRecordRequest);
      return 1;
    } catch (Exception e) {
      LOG.warn("could not write data to stream {}", putRecordRequest.getStreamName(), e);
      return 0;
    }
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisConfig extends PluginConfig {

    @Description("The name of the Kinesis stream to output to. The stream will be created if it does not exist.")
    private String name;

    @Name(Properties.KinesisRealtimeSink.BODY_FIELD)
    @Description("Name of the field in the record that contains the data to be written to the specified stream. The " +
      "data could be in binary format as a byte array or a ByteBuffer. It can also be a String.")
    private String bodyField;

    @Name(Properties.KinesisRealtimeSink.ACCESS_ID)
    @Description("AWS access Id having access to Kinesis streams")
    private String awsAccessKey;

    @Name(Properties.KinesisRealtimeSink.ACCESS_KEY)
    @Description("AWS access key secret having access to Kinesis streams")
    private String awsAccessSecret;

    @Name(Properties.KinesisRealtimeSink.SHARD_COUNT)
    @Description("Number of shards to be created, each shard has input of 1mb/s")
    private int shardCount;

    @Name(Properties.KinesisRealtimeSink.PARTITION_KEY)
    @Description("Partition key to identify shard")
    private String partition;

    KinesisConfig(String name, String bodyField, String awsAccessKey,
                  String awsAccessSecret, String partition, int shardCount) {
      this.name = name;
      this.bodyField = bodyField;
      this.awsAccessKey = awsAccessKey;
      this.awsAccessSecret = awsAccessSecret;
      this.partition = partition;
      this.shardCount = shardCount;
    }
  }
}
