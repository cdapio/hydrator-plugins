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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.commons.HiveSchemaConverter;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * A transform to convert a {@link HCatRecord} from hive to {@link StructuredRecord}.
 */
public class HCatRecordTransformer {
  private final HCatSchema hCatSchema;
  private final Schema schema;

  /**
   * A transform to convert a {@link HCatRecord} to Hive's {@link StructuredRecord}. The given {@link Schema} and
   * {@link HCatSchema} must be compatible. To convert one schema to another and supported types
   * see {@link HiveSchemaConverter}
   */
  public HCatRecordTransformer(HCatSchema hCatSchema, Schema schema) {
    this.hCatSchema = hCatSchema;
    this.schema = schema;
  }

  /**
   * Converts a {@link HCatRecord} read from a hive table to {@link StructuredRecord} using the {@link Schema} created
   * from the {@link HCatSchema}.
   *
   * @param hCatRecord the record
   * @return the converted {@link StructuredRecord}
   */
  public StructuredRecord toRecord(HCatRecord hCatRecord) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema.Type type = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() :
        field.getSchema().getType();
      switch (type) {
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BYTES: {
          try {
            builder.set(fieldName, getSchemaCompatibleValue(hCatRecord, fieldName));
            break;
          } catch (Throwable t) {
            throw new RuntimeException(String.format("Error converting field '%s' of type %s",
                                                     fieldName, type), t);
          }
        }
        default: {
          throw new IllegalStateException(String.format("Output schema contains field '%s' with unsupported type %s.",
                                                        fieldName, type));
        }
      }
    }
    return builder.build();
  }

  /**
   * Converts the value for a field from {@link HCatRecord} to the compatible {@link Schema} type to be represented in
   * {@link StructuredRecord}. For schema conversion details and supported type see {@link HiveSchemaConverter}.
   * @param hCatRecord the record being converted to {@link StructuredRecord}
   * @param fieldName name of the field
   * @return the value for the given field which is of type compatible with {@link Schema}.
   * @throws HCatException if failed to get {@link HCatFieldSchema} for the given field
   */
  private Object getSchemaCompatibleValue(HCatRecord hCatRecord, String fieldName) throws HCatException {
    PrimitiveObjectInspector.PrimitiveCategory category = hCatSchema.get(fieldName)
      .getTypeInfo().getPrimitiveCategory();
    switch (category) {
      // Its not required to check that the schema has the same type because if the user provided  the Schema then
      // the HCatSchema was obtained through the convertor and if the user didn't the Schema was obtained through the
      // and hence the types will be same.
      case BOOLEAN:
        return hCatRecord.getBoolean(fieldName, hCatSchema);
      case BYTE:
        Byte byteValue = hCatRecord.getByte(fieldName, hCatSchema);
        return byteValue == null ? null : (int) byteValue;
      case SHORT:
        Short shortValue = hCatRecord.getShort(fieldName, hCatSchema);
        return shortValue == null ? null : (int) shortValue;
      case INT:
        return hCatRecord.getInteger(fieldName, hCatSchema);
      case LONG:
        return hCatRecord.getLong(fieldName, hCatSchema);
      case FLOAT:
        return hCatRecord.getFloat(fieldName, hCatSchema);
      case DOUBLE:
        return hCatRecord.getDouble(fieldName, hCatSchema);
      case CHAR:
        HiveChar charValue = hCatRecord.getChar(fieldName, hCatSchema);
        return charValue == null ? null : charValue.toString();
      case STRING:
        return hCatRecord.getString(fieldName, hCatSchema);
      case VARCHAR:
        HiveVarchar varcharValue = hCatRecord.getVarchar(fieldName, hCatSchema);
        return varcharValue == null ? null : varcharValue.toString();
      case BINARY:
        return hCatRecord.getByteArray(fieldName, hCatSchema);
      // We can support VOID by having Schema type as null but HCatRecord does not support VOID and since we read
      // write through HCatSchema and HCatRecord we are not supporting VOID too for consistent behavior.
      case VOID:
      case DATE:
      case TIMESTAMP:
      case DECIMAL:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s. " +
                                                           "To read this table you should provide input schema in " +
                                                           "which this field is dropped.", fieldName, category.name()));
    }
  }
}
