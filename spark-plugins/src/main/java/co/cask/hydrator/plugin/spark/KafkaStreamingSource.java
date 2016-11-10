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
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Kafka")
@Description("Kafka streaming source.")
public class KafkaStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSource.class);
  private final KafkaConfig conf;

  public KafkaStreamingSource(KafkaConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    conf.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.getSchema());
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    registerUsage(context);

    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", conf.getBrokers());

    Map.Entry<String, Integer> firstBroker = conf.getBrokerMap().entrySet().iterator().next();
    String brokerHost = firstBroker.getKey();
    Integer brokerPort = firstBroker.getValue();
    SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, 20 * 1000, 128 * 1024, "partitionLookup");

    try {
      Map<TopicAndPartition, Long> offsets = conf.getInitialPartitionOffsets(getPartitions(consumer));
      // KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
      // so we have to replace them with the actual smallest and latest
      Map<TopicAndPartition, Long> updates = new HashMap<>();
      for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
        TopicAndPartition topicAndPartition = entry.getKey();
        Long offset = entry.getValue();
        if (offset == OffsetRequest.EarliestTime() || offset == OffsetRequest.LatestTime()) {
          updates.put(topicAndPartition, getOffset(consumer, topicAndPartition, offset));
        }
      }
      offsets.putAll(updates);
      LOG.info("Using initial offsets {}", offsets);

      return KafkaUtils.createDirectStream(
        context.getSparkStreamingContext(), byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
        MessageAndMetadata.class, kafkaParams, offsets,
        new Function<MessageAndMetadata<byte[], byte[]>, MessageAndMetadata>() {
          @Override
          public MessageAndMetadata call(MessageAndMetadata<byte[], byte[]> in) throws Exception {
            return in;
          }
        }).transform(new RecordTransform(conf));
    } finally {
      consumer.close();
    }
  }

  private long getOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition, long before) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(before, 1));
    kafka.javaapi.OffsetRequest offsetRequest =
      new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), "offsetLookup");
    OffsetResponse response = consumer.getOffsetsBefore(offsetRequest);
    if (response.hasError()) {
      throw new RuntimeException(String.format(
        "Unable to get offset information for partition %d. Error Code: %d",
        topicAndPartition.partition(), response.errorCode(topicAndPartition.topic(), topicAndPartition.partition())));
    }
    return response.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
  }

  private Set<Integer> getPartitions(SimpleConsumer consumer) {
    Set<Integer> partitions = conf.getPartitions();
    if (!partitions.isEmpty()) {
      return partitions;
    }

    TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(conf.getTopic()));
    TopicMetadataResponse response = consumer.send(topicMetadataRequest);
    for (TopicMetadata topicMetadata : response.topicsMetadata()) {
      for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
        partitions.add(partitionMetadata.partitionId());
      }
    }

    return partitions;
  }

  /**
   * Applies the format function to each rdd.
   */
  private static class RecordTransform
    implements Function2<JavaRDD<MessageAndMetadata>, Time, JavaRDD<StructuredRecord>> {

    private final KafkaConfig conf;

    RecordTransform(KafkaConfig conf) {
      this.conf = conf;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<MessageAndMetadata> input, Time batchTime) throws Exception {
      Function<MessageAndMetadata, StructuredRecord> recordFunction = conf.getFormat() == null ?
        new BytesFunction(batchTime.milliseconds(), conf) : new FormatFunction(batchTime.milliseconds(), conf);
      return input.map(recordFunction);
    }
  }

  /**
   * Common logic for transforming kafka key, message, partition, and offset into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private abstract static class BaseFunction implements Function<MessageAndMetadata, StructuredRecord> {
    private final long ts;
    protected final KafkaConfig conf;
    private transient String messageField;
    private transient String timeField;
    private transient String keyField;
    private transient String partitionField;
    private transient String offsetField;
    private transient Schema schema;

    BaseFunction(long ts, KafkaConfig conf) {
      this.ts = ts;
      this.conf = conf;
    }

    @Override
    public StructuredRecord call(MessageAndMetadata in) throws Exception {
      // first time this was called, initialize schema and time, key, and message fields.
      if (schema == null) {
        schema = conf.getSchema();
        timeField = conf.getTimeField();
        keyField = conf.getKeyField();
        partitionField = conf.getPartitionField();
        offsetField = conf.getOffsetField();
        for (Schema.Field field : schema.getFields()) {
          String name = field.getName();
          if (!name.equals(timeField) && !name.equals(keyField)) {
            messageField = name;
            break;
          }
        }
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      if (timeField != null) {
        builder.set(timeField, ts);
      }
      if (keyField != null) {
        builder.set(keyField, in.key());
      }
      if (partitionField != null) {
        builder.set(partitionField, in.partition());
      }
      if (offsetField != null) {
        builder.set(offsetField, in.offset());
      }
      addMessage(builder, messageField, (byte[]) in.message());
      return builder.build();
    }

    protected abstract void addMessage(StructuredRecord.Builder builder, String messageField,
                                       byte[] message) throws Exception;
  }

  /**
   * Transforms kafka key and message into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class BytesFunction extends BaseFunction {

    BytesFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) {
      builder.set(messageField, message);
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction extends BaseFunction {
    private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

    FormatFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) throws Exception {
      // first time this was called, initialize record format
      if (recordFormat == null) {
        Schema messageSchema = conf.getMessageSchema();
        FormatSpecification spec =
          new FormatSpecification(conf.getFormat(), messageSchema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(message)));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
  }

}
