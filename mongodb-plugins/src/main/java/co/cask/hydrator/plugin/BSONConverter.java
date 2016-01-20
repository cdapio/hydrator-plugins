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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.io.IOException;
import java.util.List;

/**
 * Converts {@link BSONObject} to {@link StructuredRecord}.
 */
public class BSONConverter {
  private static final List<Schema.Type> VALID_TYPES = Lists.newArrayList(Schema.Type.ARRAY, Schema.Type.BOOLEAN,
                                                                          Schema.Type.BYTES, Schema.Type.STRING,
                                                                          Schema.Type.DOUBLE,
                                                                          Schema.Type.FLOAT, Schema.Type.INT,
                                                                          Schema.Type.LONG, Schema.Type.NULL);
  private final Schema schema;

  public BSONConverter(Schema schema) throws IOException {
    this.schema = schema;
  }

  public StructuredRecord transform(BSONObject bsonObject) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), extractValue(bsonObject.get(field.getName()), field.getSchema()));
    }
    return builder.build();
  }

  public static void validateSchema(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      Schema.Type type = field.getSchema().getType();
      if (field.getSchema().isNullable()) {
        type = field.getSchema().getNonNullable().getType();
      }
      if (!VALID_TYPES.contains(type)) {
        throw new IllegalArgumentException(String.format("Unsupported Type : Field Name : %s; Type : %s",
                                                         field.getName(), field.getSchema().getType()));
      }
    }
  }

  private Object extractValue(Object object, Schema schema) {
    if (schema.isNullable()) {
      if (object == null) {
        return null;
      }
      schema = schema.getNonNullable();
    }
    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case ARRAY:
        return convertArray(object, schema.getComponentSchema());
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
        return object;
      case NULL:
        return null;
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  private Object convertArray(Object object, Schema schema) {
    BasicBSONList bsonList = (BasicBSONList) object;
    List<Object> values = Lists.newArrayListWithCapacity(bsonList.size());
    for (Object obj : bsonList) {
      values.add(extractValue(obj, schema));
    }
    return values;
  }
}
