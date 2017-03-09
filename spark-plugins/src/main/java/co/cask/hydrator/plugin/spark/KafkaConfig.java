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
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import kafka.common.TopicAndPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Conf for Kafka streaming source.
 */
@SuppressWarnings("unused")
public class KafkaConfig extends ReferencePluginConfig implements Serializable {

  private static final long serialVersionUID = 8069169417140954175L;

  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private String brokers;

  @Description("Kafka topic to read from.")
  @Macro
  private String topic;

  @Description("The topic partitions to read from. If not specified, all partitions will be read.")
  @Nullable
  @Macro
  private String partitions;

  @Description("The initial offset for each topic partition. If this is not specified, " +
    "all partitions will have the same initial offset, which is determined by the defaultInitialOffset property. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.")
  @Nullable
  @Macro
  private String initialPartitionOffsets;

  @Description("The default initial offset for all topic partitions. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. " +
    "If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property.")
  @Nullable
  @Macro
  private Long defaultInitialOffset;

  @Description("Output schema of the source, including the timeField, keyField, partitionField and offsetField. " +
    "The fields excluding the timeField and keyField are used in conjunction with the format " +
    "to parse Kafka payloads.")
  private String schema;

  @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
    "If no format is given, Kafka message payloads will be treated as bytes.")
  @Nullable
  private String format;

  @Description("Optional name of the field containing the read time of the batch. " +
    "If this is not set, no time field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private String timeField;

  @Description("Optional name of the field containing the message key. " +
    "If this is not set, no key field will be added to output records. " +
    "If set, this field must be present in the schema property and must be bytes.")
  @Nullable
  private String keyField;

  @Description("Optional name of the field containing the kafka partition that was read from. " +
    "If this is not set, no partition field will be added to output records. " +
    "If set, this field must be present in the schema property and must be an integer.")
  @Nullable
  private String partitionField;

  @Description("Optional name of the field containing the kafka offset that the message was read from. " +
    "If this is not set, no offset field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private String offsetField;

  public KafkaConfig() {
    super("");
    defaultInitialOffset = -1L;
  }

  @VisibleForTesting
  public KafkaConfig(String referenceName, String brokers, String topic, String schema, String format,
                     String timeField, String keyField, String partitionField, String offsetField) {
    this(referenceName, brokers, topic, null, null, null, schema, format,
         timeField, keyField, partitionField, offsetField);
  }

  public KafkaConfig(String referenceName, String brokers, String topic, String partitions,
                     String initialPartitionOffsets, Long defaultInitialOffset, String schema, String format,
                     String timeField, String keyField, String partitionField, String offsetField) {
    super(referenceName);
    this.brokers = brokers;
    this.topic = topic;
    this.partitions = partitions;
    this.initialPartitionOffsets = initialPartitionOffsets;
    this.defaultInitialOffset = defaultInitialOffset;
    this.schema = schema;
    this.format = format;
    this.timeField = timeField;
    this.keyField = keyField;
    this.partitionField = partitionField;
    this.offsetField = offsetField;
  }

  public String getTopic() {
    return topic;
  }

  public String getBrokers() {
    return brokers;
  }

  @Nullable
  public String getTimeField() {
    return Strings.isNullOrEmpty(timeField) ? null : timeField;
  }

  @Nullable
  public String getKeyField() {
    return Strings.isNullOrEmpty(keyField) ? null : keyField;
  }

  @Nullable
  public String getPartitionField() {
    return Strings.isNullOrEmpty(partitionField) ? null : partitionField;
  }

  @Nullable
  public String getOffsetField() {
    return Strings.isNullOrEmpty(offsetField) ? null : offsetField;
  }

  @Nullable
  public String getFormat() {
    return Strings.isNullOrEmpty(format) ? null : format;
  }

  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
    }
  }

  // gets the message schema from the schema field. If the time, key, partition, or offset fields are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema() {
    Schema schema = getSchema();
    List<Schema.Field> messageFields = new ArrayList<>();
    boolean timeFieldExists = false;
    boolean keyFieldExists = false;
    boolean partitionFieldExists = false;
    boolean offsetFieldExists = false;

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      // if the field is not the time field and not the key field, it is a message field.
      if (fieldName.equals(timeField)) {
        if (fieldType != Schema.Type.LONG) {
          throw new IllegalArgumentException("The time field must be of type long or nullable long.");
        }
        timeFieldExists = true;
      } else if (fieldName.equals(keyField)) {
        if (fieldType != Schema.Type.BYTES) {
          throw new IllegalArgumentException("The key field must be of type bytes or nullable bytes.");
        }
        keyFieldExists = true;
      } else if (fieldName.equals(partitionField)) {
        if (fieldType != Schema.Type.INT) {
          throw new IllegalArgumentException("The partition field must be of type int.");
        }
        partitionFieldExists = true;
      } else if (fieldName.equals(offsetField)) {
        if (fieldType != Schema.Type.LONG) {
          throw new IllegalArgumentException("The offset field must be of type long.");
        }
        offsetFieldExists = true;
      } else {
        messageFields.add(field);
      }
    }
    if (messageFields.isEmpty()) {
      throw new IllegalArgumentException(
        "Schema must contain at least one other field besides the time and key fields.");
    }

    if (getTimeField() != null && !timeFieldExists) {
      throw new IllegalArgumentException(String.format(
        "timeField '%s' does not exist in the schema. Please add it to the schema.", timeField));
    }
    if (getKeyField() != null && !keyFieldExists) {
      throw new IllegalArgumentException(String.format(
        "keyField '%s' does not exist in the schema. Please add it to the schema.", keyField));
    }
    if (getPartitionField() != null && !partitionFieldExists) {
      throw new IllegalArgumentException(String.format(
        "partitionField '%s' does not exist in the schema. Please add it to the schema.", partitionField));
    }
    if (getOffsetField() != null && !offsetFieldExists) {
      throw new IllegalArgumentException(String.format(
        "offsetField '%s' does not exist in the schema. Please add it to the schema.", offsetFieldExists));
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  /**
   * Get the initial partition offsets for the specified partitions. If an initial offset is specified in the
   * initialPartitionOffsets property, that value will be used. Otherwise, the defaultInitialOffset will be used.
   *
   * @param partitionsToRead the partitions to read
   * @return initial partition offsets.
   */
  public Map<TopicAndPartition, Long> getInitialPartitionOffsets(Set<Integer> partitionsToRead) {
    Map<TopicAndPartition, Long> partitionOffsets = new HashMap<>();

    // set default initial partitions
    for (Integer partition : partitionsToRead) {
      partitionOffsets.put(new TopicAndPartition(topic, partition), defaultInitialOffset);
    }

    // if initial partition offsets are specified, overwrite the defaults.
    if (initialPartitionOffsets != null) {
      for (KeyValue<String, String> partitionAndOffset : KeyValueListParser.DEFAULT.parse(initialPartitionOffsets)) {
        String partitionStr = partitionAndOffset.getKey();
        String offsetStr = partitionAndOffset.getValue();
        int partition;
        try {
          partition = Integer.parseInt(partitionStr);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(String.format(
            "Invalid partition '%s' in initialPartitionOffsets.", partitionStr));
        }
        long offset;
        try {
          offset = Long.parseLong(offsetStr);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(String.format(
            "Invalid offset '%s' in initialPartitionOffsets for partition %d.", partitionStr, partition));
        }
        partitionOffsets.put(new TopicAndPartition(topic, partition), offset);
      }
    }

    return partitionOffsets;
  }

  /**
   * @return broker host to broker port mapping.
   */
  public Map<String, Integer> getBrokerMap() {
    Map<String, Integer> brokerMap = new HashMap<>();
    for (KeyValue<String, String> hostAndPort : KeyValueListParser.DEFAULT.parse(brokers)) {
      String host = hostAndPort.getKey();
      String portStr = hostAndPort.getValue();
      try {
        brokerMap.put(host, Integer.parseInt(portStr));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid port '%s' for host '%s'.", portStr, host));
      }
    }
    if (brokerMap.isEmpty()) {
      throw new IllegalArgumentException("Must specify kafka brokers.");
    }
    return brokerMap;
  }

  /**
   * @return set of partitions to read from. Returns an empty list if no partitions were specified.
   */
  public Set<Integer> getPartitions() {
    Set<Integer> partitionSet = new HashSet<>();
    if (partitions == null) {
      return partitionSet;
    }
    for (String partition : Splitter.on(',').trimResults().split(partitions)) {
      try {
        partitionSet.add(Integer.parseInt(partition));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          String.format("Invalid partition '%s'. Partitions must be integers.", partition));
      }
    }
    return partitionSet;
  }

  public void validate() {
    getBrokerMap();
    getPartitions();
    getInitialPartitionOffsets(getPartitions());

    if (!Strings.isNullOrEmpty(timeField) && !Strings.isNullOrEmpty(keyField) && timeField.equals(keyField)) {
      throw new IllegalArgumentException(String.format(
        "The timeField and keyField cannot both have the same name (%s).", timeField));
    }

    Schema messageSchema = getMessageSchema();
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
}
