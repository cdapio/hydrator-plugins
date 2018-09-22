/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.hydrator.common;

import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MICROS;
import static co.cask.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MILLIS;

/**
 * Converts an object with a schema into another type of object with the same schema.
 * For example, child implementations could convert a StructuredRecord to a GenericRecord and vice versa
 * as can be seen in AvroToStructuredTransformer and StructuredToAvroTransformer
 *
 * @param <INPUT> type of input record
 * @param <OUTPUT> type of output record
 */
public abstract class RecordConverter<INPUT, OUTPUT> {
  protected boolean convertTimestampToMillis;
  protected boolean convertTimestampToMicros;

  public RecordConverter() {
    this(false, false);
  }

  public RecordConverter(boolean convertMicrosToMillis, boolean convertTimestampToMicros) {
    this.convertTimestampToMillis = convertMicrosToMillis;
    this.convertTimestampToMicros = convertTimestampToMicros;
  }

  public abstract OUTPUT transform(INPUT record, Schema schema) throws IOException;

  private Object convertUnion(Object value, List<Schema> schemas) {
    boolean isNullable = false;
    for (Schema possibleSchema : schemas) {
      if (possibleSchema.getType() == Schema.Type.NULL) {
        isNullable = true;
        if (value == null) {
          return value;
        }
      } else {
        try {
          return convertField(value, possibleSchema);
        } catch (Exception e) {
          // if we couldn't convert, move to the next possibility
        }
      }
    }
    if (isNullable) {
      return null;
    }
    throw new UnexpectedFormatException("unable to determine union type.");
  }

  private List<Object> convertArray(Object values, Schema elementSchema) throws IOException {
    List<Object> output;
    if (values instanceof List) {
      List<Object> valuesList = (List<Object>) values;
      output = Lists.newArrayListWithCapacity(valuesList.size());
      for (Object value : valuesList) {
        output.add(convertField(value, elementSchema));
      }
    } else {
      int length = Array.getLength(values);
      output = Lists.newArrayListWithCapacity(length);
      for (int i = 0; i < length; i++) {
        output.add(convertField(Array.get(values, i), elementSchema));
      }
    }
    return output;
  }

  private Map<Object, Object> convertMap(Map<Object, Object> map, Schema keySchema,
                                         Schema valueSchema) throws IOException {
    Map<Object, Object> converted = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      converted.put(convertField(entry.getKey(), keySchema), convertField(entry.getValue(), valueSchema));
    }
    return converted;
  }

  protected Object convertField(Object field, Schema fieldSchema) throws IOException {
    Schema.Type fieldType = fieldSchema.getType();
    // AvroSerDe does not support timestamp-micros. So convert schema with timestamp-micros to timestamp-millis and
    // vice versa
    if (convertTimestampToMillis && fieldSchema.getLogicalType() == TIMESTAMP_MICROS) {
      return TimeUnit.MICROSECONDS.toMillis((Long) field);
    }

    if (convertTimestampToMicros && fieldSchema.getLogicalType() == TIMESTAMP_MILLIS) {
      return TimeUnit.MILLISECONDS.toMicros((Long) field);
    }

    switch (fieldType) {
      case RECORD:
        return transform((INPUT) field, fieldSchema);
      case ARRAY:
        return convertArray(field, fieldSchema.getComponentSchema());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = fieldSchema.getMapSchema();
        return convertMap((Map<Object, Object>) field, mapSchema.getKey(), mapSchema.getValue());
      case UNION:
        return convertUnion(field, fieldSchema.getUnionSchemas());
      case NULL:
        return null;
      case STRING:
        return field.toString();
      case BYTES:
        return convertBytes(field);
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return field;
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  protected Object convertBytes(Object field) {
    return field;
  }
}
