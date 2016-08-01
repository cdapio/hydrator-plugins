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

package co.cask.hydrator.plugin.batch.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Conf for Kafka streaming source.
 */
@SuppressWarnings("unused")
public class KafkaConfig extends ReferencePluginConfig implements Serializable {

  private static final long serialVersionUID = 8069169417140954175L;

  @Description("Comma-separated list of Kafka brokers specified in host1:port1,host2:port2 form.")
  private String brokers;

  @Description("Comma-separated list of Kafka topics to read from.")
  private String topics;

  @Description("Output schema of the source, including the timeField and keyField. " +
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
    "If set, this field must be present in the schema property.")
  @Nullable
  private String timeField;

  @Description("Optional name of the field containing the message key. " +
    "If this is not set, no key field will be added to output records. " +
    "If set, this field must be present in the schema property.")
  @Nullable
  private String keyField;

  public KafkaConfig() {
    super("");
    timeField = null;
    keyField = null;
  }

  @VisibleForTesting
  public KafkaConfig(String referenceName, String brokers, String topics, String schema, String format,
                     String timeField, String keyField) {
    super(referenceName);
    this.brokers = brokers;
    this.topics = topics;
    this.schema = schema;
    this.format = format;
    this.timeField = timeField;
    this.keyField = keyField;
  }

  public Set<String> getTopics() {
    Set<String> topicSet = new HashSet<>();
    for (String topic : Splitter.on(',').trimResults().split(topics)) {
      topicSet.add(topic);
    }
    return topicSet;
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

  // gets the message schema from the schema field. If the time field and key field are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema() {
    Schema schema = getSchema();
    List<Schema.Field> messageFields = new ArrayList<>();
    boolean timeFieldExists = false;
    boolean keyFieldExists = false;
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      // if the field is not the time field and not the key field, it is a message field.
      if (fieldName.equals(timeField)) {
        Schema.Type timeType = fieldSchema.isNullable() ?
          fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (timeType != Schema.Type.LONG) {
          throw new IllegalArgumentException("The time field must be of type long or nullable long.");
        }
        timeFieldExists = true;
      } else if (fieldName.equals(keyField)) {
        Schema.Type timeType = fieldSchema.isNullable() ?
          fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (timeType != Schema.Type.BYTES) {
          throw new IllegalArgumentException("The key field must be of type bytes or nullable bytes.");
        }
        keyFieldExists = true;
      } else {
        messageFields.add(field);
      }
    }
    if (messageFields.isEmpty()) {
      throw new IllegalArgumentException(
        "Schema must contain at least one other field besides the time and key fields.");
    }

    if (getTimeField() != null && !timeFieldExists) {
      throw new IllegalArgumentException(String.format("timeField '%s' does not exist in the schema.", timeField));
    }
    if (getKeyField() != null && !keyFieldExists) {
      throw new IllegalArgumentException(String.format("keyField '%s' does not exist in the schema.", keyField));
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  public void validate() {
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
