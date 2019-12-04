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
 * Provides Blob formatters.
 */
public class BlobInputProvider implements FileInputFormatterProvider {

  @Nullable
  @Override
  public Schema getSchema(@Nullable String pathField, String filePath) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
    if (pathField != null && !pathField.isEmpty()) {
      fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf("blob", fields);
  }

  @Override
  public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
    String pathField = properties.get(FileSourceProperties.PATH_FIELD);
    if (schema == null) {
      return new BlobInputFormatter(getSchema(pathField, null));
    }

    Schema.Field bodyField = schema.getField("body");
    if (bodyField == null) {
      throw new IllegalArgumentException("The schema for the 'blob' format must have a field named 'body'");
    }
    Schema bodySchema = bodyField.getSchema();
    Schema.Type bodyType = bodySchema.isNullable() ? bodySchema.getNonNullable().getType() : bodySchema.getType();
    if (bodyType != Schema.Type.BYTES) {
      throw new IllegalArgumentException(String.format("The 'body' field must be of type 'bytes', but found '%s'",
                                                       bodyType.name().toLowerCase()));
    }

    // blob must contain 'body' as type 'bytes'.
    // it can optionally contain a path field of type 'string'
    int numExpectedFields = pathField == null ? 1 : 2;
    int numFields = schema.getFields().size();
    if (numFields > numExpectedFields) {
      int numExtra = numFields - numExpectedFields;
      if (pathField == null) {
        throw new IllegalArgumentException(
          String.format("The schema for the 'blob' format must only contain the 'body' field, "
                          + "but found %d other field%s.", numFields - 1, numExtra > 1 ? "s" : ""));
      } else {
        throw new IllegalArgumentException(
          String.format("The schema for the 'blob' format must only contain the 'body' field and the '%s' field, "
                          + "but found %d other field%s.", pathField, numFields - 2, numExtra > 1 ? "s" : ""));
      }
    }

    return new BlobInputFormatter(schema);
  }
}
