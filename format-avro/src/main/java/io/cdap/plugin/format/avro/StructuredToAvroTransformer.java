
/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.avro;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.common.RecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates GenericRecords from StructuredRecords
 */
public class StructuredToAvroTransformer extends RecordConverter<StructuredRecord, GenericRecord> {

  private final Map<Integer, Schema> schemaCache;
  private final io.cdap.cdap.api.data.schema.Schema outputCDAPSchema;

  public StructuredToAvroTransformer(@Nullable io.cdap.cdap.api.data.schema.Schema outputSchema) {
    this.schemaCache = Maps.newHashMap();
    this.outputCDAPSchema = outputSchema;
  }

  public GenericRecord transform(StructuredRecord structuredRecord) throws IOException {
    return transform(structuredRecord,
                     outputCDAPSchema == null ? structuredRecord.getSchema() : outputCDAPSchema);
  }

  @Override
  public GenericRecord transform(StructuredRecord structuredRecord,
                                 io.cdap.cdap.api.data.schema.Schema schema) throws IOException {
    io.cdap.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

    Schema avroSchema = getAvroSchema(schema);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      io.cdap.cdap.api.data.schema.Schema.Field schemaField = structuredRecordSchema.getField(fieldName);
      if (schemaField == null) {
        throw new IllegalArgumentException("Input record does not contain the " + fieldName + " field.");
      }
      recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), schemaField));
    }
    return recordBuilder.build();
  }

  @Override
  protected Object convertBytes(Object field) {
    if (field instanceof ByteBuffer) {
      return field;
    }
    return ByteBuffer.wrap((byte[]) field);
  }

  private Schema getAvroSchema(io.cdap.cdap.api.data.schema.Schema cdapSchema) {
    int hashCode = cdapSchema.hashCode();
    if (schemaCache.containsKey(hashCode)) {
      return schemaCache.get(hashCode);
    } else {
      Schema avroSchema = new Schema.Parser().parse(cdapSchema.toString());
      schemaCache.put(hashCode, avroSchema);
      return avroSchema;
    }
  }
}
