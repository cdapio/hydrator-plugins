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
import co.cask.hydrator.plugin.common.KVTableSinkConfig;
import co.cask.hydrator.plugin.common.KVTableSinkUtil;
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

  private final KVTableSinkConfig kvTableSinkConfig;

  public RealtimeKVTableSink(KVTableSinkConfig kvTableSinkConfig) {
    this.kvTableSinkConfig = kvTableSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    KVTableSinkUtil.configureKVTablePipeline(pipelineConfigurer, kvTableSinkConfig);
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter writer) throws Exception {
    KeyValueTable table = writer.getDataset(kvTableSinkConfig.getName());
    int numRecords = 0;
    for (StructuredRecord record : records) {
      // get key bytes
      Object key = record.get(kvTableSinkConfig.getKeyField());
      Preconditions.checkArgument(key != null, "Key cannot be null.");
      byte[] keyBytes;

      Schema keyFieldSchema = record.getSchema().getField(kvTableSinkConfig.getKeyField()).getSchema();
      if (keyFieldSchema.getType().equals(Schema.Type.STRING)) {
        keyBytes = Bytes.toBytes((String) key);
      } else if (keyFieldSchema.getType().equals(Schema.Type.BYTES)) {
          keyBytes = key instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) key) : (byte[]) key;
      } else {
        throw new Exception(
          String.format("Key field %s cannot have schema %s. It must be of either String or Bytes",
                        kvTableSinkConfig.getKeyField(), keyFieldSchema));
      }

      // get value bytes
      Schema.Field valueFieldSchema = record.getSchema().getField(kvTableSinkConfig.getValueField());
      if (valueFieldSchema == null) {
        throw new Exception(
          String.format("Value Field %s is missing in the input record", kvTableSinkConfig.getValueField()));
      }

      byte[] valBytes = null;
      Object val = record.get(kvTableSinkConfig.getValueField());
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
                          kvTableSinkConfig.getValueField(), valueFieldSchema));
        }
      }

      table.write(new KeyValue<byte[], byte[]>(keyBytes, valBytes));
      numRecords++;
    }
    return numRecords;
  }

}
