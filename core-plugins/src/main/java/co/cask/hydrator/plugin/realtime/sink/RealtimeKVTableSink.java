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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Real-time sink for KV Table
 */
@Plugin(type = "realtimesink")
@Name("KVTable")
@Description("Real-time Sink for CDAP KVTable dataset")
public class RealtimeKVTableSink extends RealtimeSink<StructuredRecord> {

  private static final String NAME_DESC = "Name of the dataset. If it does not already exist, one will be created.";
  private static final String KEY_FIELD_DESC = "The name of the field to use as the key. Defaults to 'key'.";
  private static final String VALUE_FIELD_DESC = "The name of the field to use as the value. Defaults to 'value'.";

  /**
   * Config class for KVTableSink
   */
  public static class RealtimeKVTableSinkConfig extends PluginConfig {
    @Description(NAME_DESC)
    private String name;

    @Name(Properties.KeyValueTable.KEY_FIELD)
    @Description(KEY_FIELD_DESC)
    @Nullable
    private String keyField;

    @Name(Properties.KeyValueTable.VALUE_FIELD)
    @Description(VALUE_FIELD_DESC)
    @Nullable
    private String valueField;

    public RealtimeKVTableSinkConfig() {
      this(null, Properties.KeyValueTable.DEFAULT_KEY_FIELD, Properties.KeyValueTable.DEFAULT_VALUE_FIELD);
    }

    public RealtimeKVTableSinkConfig(String name, String keyField, String valueField) {
      this.name = name;
      this.keyField = keyField;
      this.valueField = valueField;
    }
  }

  private final RealtimeKVTableSinkConfig realtimeKVTableSinkConfig;
  private RecordPutTransformer recordPutTransformer;

  public RealtimeKVTableSink(RealtimeKVTableSinkConfig realtimeKVTableConfig) {
    this.realtimeKVTableSinkConfig = realtimeKVTableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    // validate that input and output fields are present
    if (inputSchema != null) {
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, realtimeKVTableSinkConfig.keyField);
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, realtimeKVTableSinkConfig.valueField);
      validateSchemaTypeIsStringOrBytes(inputSchema, realtimeKVTableSinkConfig.keyField, false);
      validateSchemaTypeIsStringOrBytes(inputSchema, realtimeKVTableSinkConfig.valueField, true);
    }
  }

  private void validateSchemaTypeIsStringOrBytes(Schema inputSchema, String fieldName, boolean isNullable) {
    Schema fieldSchema = inputSchema.getField(fieldName).getSchema();
    boolean fieldIsNullable = fieldSchema.isNullable();
    if (!isNullable && fieldIsNullable) {
      throw new IllegalArgumentException("Field " + fieldName + " cannot be nullable");
    }
    Schema.Type fieldType = fieldIsNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (fieldType != Schema.Type.STRING && fieldType != Schema.Type.BYTES) {
      throw new IllegalArgumentException(
        String.format("Field name %s is of type %s, only types String and Bytes are supported for KVTable",
                      fieldName, fieldType));
    }
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter writer) throws Exception {
    KeyValueTable table = writer.getDataset(realtimeKVTableSinkConfig.name);
    int numRecords = 0;
    for (StructuredRecord record : records) {
      // get key bytes
      Object key = record.get(realtimeKVTableSinkConfig.keyField);
      Preconditions.checkArgument(key != null, "Key cannot be null.");
      byte[] keyBytes;

      Schema keyFieldSchema = record.getSchema().getField(realtimeKVTableSinkConfig.keyField).getSchema();
      if (keyFieldSchema.getType().equals(Schema.Type.STRING)) {
        keyBytes = Bytes.toBytes((String) key);
      } else if (keyFieldSchema.getType().equals(Schema.Type.BYTES)) {
          keyBytes = key instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) key) : (byte[]) key;
      } else {
        throw new Exception(
          String.format("Key field %s cannot have schema %s. It must be of either String or Bytes",
                        realtimeKVTableSinkConfig.keyField, keyFieldSchema));
      }

      // get value bytes
      Schema.Field valueFieldSchema = record.getSchema().getField(realtimeKVTableSinkConfig.valueField);
      if (valueFieldSchema == null) {
        throw new Exception(
          String.format("Value Field %s is missing in the input record", realtimeKVTableSinkConfig.valueField));
      }

      byte[] valBytes = null;
      Object val = record.get(realtimeKVTableSinkConfig.valueField);
      if (val != null) {
        Schema.Type valueFieldType =
          valueFieldSchema.getSchema().isNullable() ? valueFieldSchema.getSchema().getNonNullable().getType() :
            valueFieldSchema.getSchema().getType();
        if (valueFieldType.equals(Schema.Type.STRING)) {
          valBytes = Bytes.toBytes((String) val);
        } else if (valueFieldType.equals(Schema.Type.BYTES)) {
          valBytes = val instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) val) : (byte[]) val;
        } else {
          throw new Exception(
            String.format("Value field %s cannot have schema %s. It must be of either String or Bytes",
                          realtimeKVTableSinkConfig.valueField, valueFieldSchema));
        }
      }

      table.write(new KeyValue<byte[], byte[]>(keyBytes, valBytes));
      numRecords++;
    }
    return numRecords;
  }

}
