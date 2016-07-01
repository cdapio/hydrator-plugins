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
import co.cask.hydrator.plugin.common.KVTableSinkConfig;
import co.cask.hydrator.plugin.common.KVTableSinkUtil;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * CDAP KVTable Dataset Batch Sink.
 */
@Plugin(type = "batchsink")
@Name("KVTable")
@Description("Writes records to a KeyValueTable, using configurable fields from input records as the key and value.")
public class KVTableSink extends BatchWritableSink<StructuredRecord, byte[], byte[]> {

  private final KVTableSinkConfig kvTableConfig;

  public KVTableSink(KVTableSinkConfig kvTableConfig) {
    this.kvTableConfig = kvTableConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    KVTableSinkUtil.configureKVTablePipeline(pipelineConfigurer, kvTableConfig);
  }

  @Override
  protected Map<String, String> getProperties() {

    Map<String, String> properties;
    // will be null only in tests
    if (kvTableConfig.getProperties() == null) {
      properties = new HashMap<>();
    } else {
      properties = new HashMap<>(kvTableConfig.getProperties().getProperties());
    }
    properties.put(Properties.BatchReadableWritable.NAME, kvTableConfig.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, KeyValueTable.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], byte[]>> emitter) throws Exception {
    Object key = input.get(kvTableConfig.getKeyField());
    Preconditions.checkArgument(key != null, "Key cannot be null.");
    byte[] keyBytes;

    Schema keyFieldSchema = input.getSchema().getField(kvTableConfig.getKeyField()).getSchema();
    if (keyFieldSchema.getType().equals(Schema.Type.STRING)) {
      keyBytes = Bytes.toBytes((String) key);
    } else if (keyFieldSchema.getType().equals(Schema.Type.BYTES)) {
      keyBytes = key instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) key) : (byte[]) key;
    } else if (keyFieldSchema.isNullable()) {
      throw new Exception(
        String.format("Key field %s cannot have nullable schema %s", kvTableConfig.getKeyField(), keyFieldSchema));
    } else {
      throw new Exception(
        String.format("Key field %s cannot have schema %s. It must of either String or Bytes",
                      kvTableConfig.getKeyField(), keyFieldSchema));
    }

    Schema.Field valueFieldSchema = input.getSchema().getField(kvTableConfig.getValueField());
    if (valueFieldSchema == null) {
      throw new Exception("Value Field " + kvTableConfig.getValueField() + " is missing in the input record");
    }

    byte[] valBytes = null;
    Object val = input.get(kvTableConfig.getValueField());
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
                        kvTableConfig.getValueField(), valueFieldSchema));
      }
    }
    emitter.emit(new KeyValue<>(keyBytes, valBytes));
  }
}
