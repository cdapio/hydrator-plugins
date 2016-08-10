
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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.hydrator.common.RecordConverter;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Creates GenericRecords from StructuredRecords
 */
public class StructuredToAvroTransformer extends RecordConverter<StructuredRecord, GenericRecord> {

  private final Map<Integer, Schema> schemaCache;
  private final co.cask.cdap.api.data.schema.Schema outputCDAPSchema;
  private final Schema outputAvroSchema;

  public StructuredToAvroTransformer(String outputSchema) {
    this.schemaCache = Maps.newHashMap();
    try {
      this.outputCDAPSchema =
        (outputSchema != null) ? co.cask.cdap.api.data.schema.Schema.parseJson(outputSchema) : null;
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: Reason: " + e.getMessage(), e);
    }
    this.outputAvroSchema = (outputSchema != null) ? new Schema.Parser().parse(outputSchema) : null;
  }

  public GenericRecord transform(StructuredRecord structuredRecord) throws IOException {
    return transform(structuredRecord,
                     outputCDAPSchema == null ? structuredRecord.getSchema() : outputCDAPSchema);
  }

  @Override
  public GenericRecord transform(StructuredRecord structuredRecord,
                                 co.cask.cdap.api.data.schema.Schema schema) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

    Schema avroSchema = getAvroSchema(schema);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      co.cask.cdap.api.data.schema.Schema.Field schemaField = structuredRecordSchema.getField(fieldName);
      if (schemaField == null) {
        throw new IllegalArgumentException("Input record does not contain the " + fieldName + " field.");
      }
      recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), schemaField.getSchema()));
    }
    return recordBuilder.build();
  }

  private Schema getAvroSchema(co.cask.cdap.api.data.schema.Schema cdapSchema) {
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
