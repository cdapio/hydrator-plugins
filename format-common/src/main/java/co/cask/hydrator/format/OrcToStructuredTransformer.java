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

package co.cask.hydrator.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.RecordConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Create OrcStruct from GenericRecords
 */
public class OrcToStructuredTransformer extends RecordConverter<OrcStruct, StructuredRecord> {

  private static final Map<Integer, Schema> schemaCache = Maps.newHashMap();

  public StructuredRecord transform(OrcStruct orcStruct) throws IOException {
    TypeDescription orcRecordScehma = orcStruct.getSchema();
    return transform(orcStruct, convertSchema(orcRecordScehma));
  }

  @Override
  public StructuredRecord transform(OrcStruct orcStruct, Schema structuredSchema) throws IOException {
    return transform(orcStruct, structuredSchema, null).build();
  }

  public StructuredRecord.Builder transform(OrcStruct orcStruct, Schema structuredSchema,
                                            @Nullable String skipField) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : structuredSchema.getFields()) {
      String fieldName = field.getName();
      if (!fieldName.equals(skipField)) {
        builder.set(fieldName, convertField(orcStruct.getFieldValue(fieldName), field.getSchema()));
      }
    }

    return builder;
  }

  public static Schema convertSchema(TypeDescription schema) {
    int hashCode = schema.hashCode();
    Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      structuredSchema = toSchema(schema);
      schemaCache.put(hashCode, structuredSchema);
    }
    return structuredSchema;
  }

  @Override
  protected Object convertField(Object field, Schema fieldSchema) throws IOException {
    if (field == null) {
      return null;
    }
    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case RECORD:
        return transform((OrcStruct) field, fieldSchema);
      case ARRAY:
        return convertArray(field, fieldSchema.getComponentSchema());
      case UNION:
        return convertUnion(field, fieldSchema.getUnionSchemas());
      case NULL:
        return null;
      case STRING:
        return ((Text) field).toString();
      case BYTES:
        return ((BytesWritable) field).getBytes();
      case INT:
        return ((IntWritable) field).get();
      case LONG:
        return ((LongWritable) field).get();
      case FLOAT:
        return ((FloatWritable) field).get();
      case DOUBLE:
        return ((DoubleWritable) field).get();
      case BOOLEAN:
        return ((BooleanWritable) field).get();
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  // TODO: add array support
  private static Schema toSchema(TypeDescription schema) {
    List<Schema.Field> fields = Lists.newArrayList();
    List<String> fieldNames = schema.getFieldNames();
    int index = 0;
    for (TypeDescription fieldSchema : schema.getChildren()) {
      String name = fieldNames.get(index);
      if (!fieldSchema.getCategory().isPrimitive() && !fieldSchema.getCategory().equals(TypeDescription.Category.LIST)) {
        throw new IllegalArgumentException(String.format(
                "Schema contains field '%s' with complex type %s. Only primitive types are supported.",
                name, fieldSchema));
      }
      fields.add(Schema.Field.of(name, getType(fieldSchema)));
      index++;
    }
    return Schema.recordOf("record", fields);
  }

  // TODO: add array support
  private static Schema getType(TypeDescription typeDescription) {
    switch (typeDescription.getCategory()) {
      case BOOLEAN:
        return Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN));
      case BYTE:
      case SHORT:
      case INT:
        return Schema.nullableOf(Schema.of(Schema.Type.INT));
      case LONG:
        return Schema.nullableOf(Schema.of(Schema.Type.LONG));
      case FLOAT:
        return Schema.nullableOf(Schema.of(Schema.Type.FLOAT));
      case DOUBLE:
      case DECIMAL:
        return Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
      case CHAR:
      case STRING:
      case VARCHAR:
        return Schema.nullableOf(Schema.of(Schema.Type.STRING));
      case BINARY:
        return Schema.nullableOf(Schema.of(Schema.Type.BYTES));
      case LIST: {
        List<TypeDescription> typeDescriptionList = typeDescription.getChildren();
        if (typeDescriptionList != null && !typeDescriptionList.isEmpty()) {
          System.out.println("Array Type Category : " + typeDescriptionList.get(0).getCategory());
          return Schema.nullableOf(Schema.arrayOf(getType(typeDescriptionList.get(0))));
        } else {
          throw new IllegalArgumentException("Not a valid arrayType");
        }
      }
      case MAP:
      case UNION:
      case STRUCT:
      case TIMESTAMP:
      case DATE:
      default:
        throw new IllegalArgumentException(
                String.format("Schema contains field type %s which is currently not supported",
                typeDescription.getCategory().name()));
    }
  }
}
