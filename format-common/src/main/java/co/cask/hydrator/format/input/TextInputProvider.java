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
import co.cask.hydrator.format.plugin.FileSourceProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides Parquet formatters.
 */
public class TextInputProvider implements FileInputFormatterProvider {

  @Nullable
  @Override
  public Schema getSchema(@Nullable String pathField, String filePath) {
    return getDefaultSchema(pathField);
  }

  @Override
  public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
    String pathField = properties.get(FileSourceProperties.PATH_FIELD);
    if (schema == null) {
      return new TextInputFormatter(getSchema(pathField, null));
    }

    // text must contain 'body' as type 'string'.
    // it can optionally contain a 'offset' field of type 'long'
    // it can optionally contain a path field of type 'string'
    Schema.Field offsetField = schema.getField("offset");
    if (offsetField != null) {
      Schema offsetSchema = offsetField.getSchema();
      Schema.Type offsetType = offsetSchema.isNullable() ? offsetSchema.getNonNullable().getType() :
        offsetSchema.getType();
      if (offsetType != Schema.Type.LONG) {
        throw new IllegalArgumentException(String.format("The 'offset' field must be of type 'long', but found '%s'",
                                                         offsetType.name().toLowerCase()));
      }
    }

    Schema.Field bodyField = schema.getField("body");
    if (bodyField == null) {
      throw new IllegalArgumentException("The schema for the 'text' format must have a field named 'body'");
    }
    Schema bodySchema = bodyField.getSchema();
    Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
    if (bodyType != Schema.Type.STRING) {
      throw new IllegalArgumentException(String.format("The 'body' field must be of type 'string', but found '%s'",
                                                       bodyType.name().toLowerCase()));
    }

    // fields should be body (required), offset (optional), [pathfield] (optional)
    boolean expectOffset = schema.getField("offset") != null;
    boolean expectPath = pathField != null;
    int numExpectedFields = 1;
    if (expectOffset) {
      numExpectedFields++;
    }
    if (expectPath) {
      numExpectedFields++;
    }
    int maxExpectedFields = pathField == null ? 2 : 3;
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      String expectedFields;
      if (expectOffset && expectPath) {
        expectedFields = String.format("'offset', 'body', and '%s' fields", pathField);
      } else if (expectOffset) {
        expectedFields = "'offset' and 'body' fields";
      } else if (expectPath) {
        expectedFields = String.format("'body' and '%s' fields", pathField);
      } else {
        expectedFields = "'body' field";
      }

      int numExtraFields = numFields - maxExpectedFields;
      throw new IllegalArgumentException(
        String.format("The schema for the 'text' format must only contain the %s, but found %d other field%s",
                      expectedFields, numExtraFields, numExtraFields > 1 ? "s" : ""));
    }

    return new TextInputFormatter(schema);
  }

  public static Schema getDefaultSchema(@Nullable String pathField) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    if (pathField != null && !pathField.isEmpty()) {
      fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf("textfile", fields);
  }
}
