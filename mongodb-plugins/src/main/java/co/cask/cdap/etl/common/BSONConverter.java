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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.bson.BSONObject;

import java.io.IOException;

/**
 *
 */
public class BSONConverter extends RecordConverter<BSONObject, StructuredRecord> {
  private final Schema schema;
  private final org.apache.avro.Schema avroSchema;

  public BSONConverter(String schemaString) throws IOException {
    this.schema = Schema.parseJson(schemaString);
    this.avroSchema = org.apache.avro.Schema.parse(schemaString);
  }

  @Override
  public StructuredRecord transform(BSONObject bsonObject) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
      builder.set(field.name(), convertField(bsonObject.get(field.name()), field.schema()));
    }
    return builder.build();
  }
}
