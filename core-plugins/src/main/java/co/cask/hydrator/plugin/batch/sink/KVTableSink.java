/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.hydrator.common.SchemaValidator;
import co.cask.hydrator.plugin.common.BatchReadableWritableConfig;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * CDAP KVTable Dataset Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("KVTable")
@Description("Writes records to a KeyValueTable, using configurable fields from input records as the key and value.")
public class KVTableSink extends BatchWritableSink<StructuredRecord, byte[], byte[]> {

  private static final String KEY_FIELD_DESC = "The name of the field to use as the key. Defaults to 'key'.";
  private static final String VALUE_FIELD_DESC = "The name of the field to use as the value. Defaults to 'value'.";

  /**
   * Config class for KVTableSink
   */
  public static class KVTableConfig extends BatchReadableWritableConfig {

    @Name(Properties.KeyValueTable.KEY_FIELD)
    @Description(KEY_FIELD_DESC)
    @Nullable
    private String keyField;

    @Name(Properties.KeyValueTable.VALUE_FIELD)
    @Description(VALUE_FIELD_DESC)
    @Nullable
    private String valueField;

    public KVTableConfig() {
      this(null, Properties.KeyValueTable.DEFAULT_KEY_FIELD, Properties.KeyValueTable.DEFAULT_VALUE_FIELD);
    }

    public KVTableConfig(String name, String keyField, String valueField) {
      super(name);
      this.keyField = keyField;
      this.valueField = valueField;
    }
  }

  private final KVTableConfig kvTableConfig;

  public KVTableSink(KVTableConfig kvTableConfig) {
    super(kvTableConfig);
    this.kvTableConfig = kvTableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    // validate that input and output fields are present
    if (inputSchema != null) {
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, kvTableConfig.keyField);
      SchemaValidator.validateFieldsArePresentInSchema(inputSchema, kvTableConfig.valueField);
      validateSchemaTypeIsStringOrBytes(inputSchema, kvTableConfig.keyField, false);
      validateSchemaTypeIsStringOrBytes(inputSchema, kvTableConfig.valueField, true);
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
  protected Map<String, String> getProperties() {
    Map<String, String> properties;
    properties = new HashMap<>(kvTableConfig.getProperties().getProperties());

    properties.put(Properties.BatchReadableWritable.NAME, kvTableConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, KeyValueTable.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], byte[]>> emitter) throws Exception {
    Object key = input.get(kvTableConfig.keyField);
    Preconditions.checkArgument(key != null, "Key cannot be null.");
    byte[] keyBytes;

    Schema keyFieldSchema = input.getSchema().getField(kvTableConfig.keyField).getSchema();
    if (keyFieldSchema.getType().equals(Schema.Type.STRING)) {
      keyBytes = Bytes.toBytes((String) key);
    } else if (keyFieldSchema.getType().equals(Schema.Type.BYTES)) {
      keyBytes = key instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) key) : (byte[]) key;
    } else if (keyFieldSchema.isNullable()) {
      throw new Exception(
        String.format("Key field %s cannot have nullable schema %s", kvTableConfig.keyField, keyFieldSchema));
    } else {
      throw new Exception(
        String.format("Key field %s cannot have schema %s. It must of either String or Bytes",
                      kvTableConfig.keyField, keyFieldSchema));
    }

    Schema.Field valueFieldSchema = input.getSchema().getField(kvTableConfig.valueField);
    if (valueFieldSchema == null) {
      throw new Exception("Value Field " + kvTableConfig.valueField + " is missing in the input record");
    }

    byte[] valBytes = null;
    Object val = input.get(kvTableConfig.valueField);
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
          String.format("Value field %s cannot have schema %s. It must of either String or Bytes",
                        kvTableConfig.valueField, valueFieldSchema));
      }
    }
    emitter.emit(new KeyValue<>(keyBytes, valBytes));
  }
}
