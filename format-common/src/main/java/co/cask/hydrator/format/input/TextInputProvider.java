/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides Parquet formatters.
 */
public class TextInputProvider implements FileInputFormatterProvider {

  @Override
  public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
    if (schema != null) {
      Schema.Field offsetField = schema.getField("offset");
      if (offsetField != null) {
        Schema offsetSchema = offsetField.getSchema();
        Schema.Type offsetType = offsetSchema.isNullable() ? offsetSchema.getNonNullable().getType() :
          offsetSchema.getType();
        if (offsetType != Schema.Type.LONG) {
          throw new IllegalArgumentException("Type of 'offset' field must be 'long', but found " + offsetType);
        }
      }

      Schema.Field bodyField = schema.getField("body");
      if (bodyField == null) {
        throw new IllegalArgumentException("Schema for text format must have a field named 'body'");
      }
      Schema bodySchema = bodyField.getSchema();
      Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
      if (bodyType != Schema.Type.STRING) {
        throw new IllegalArgumentException("Type of 'body' field must be 'string', but found + " + bodyType);
      }
    }

    if (schema == null) {
      schema = getDefaultSchema(properties.get("pathField"));
    }
    return new TextInputFormatter(schema);
  }

  public static Schema getDefaultSchema(@Nullable String pathField) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    if (pathField != null) {
      fields.add(Schema.Field.of(pathField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("textfile", fields);
  }
}
