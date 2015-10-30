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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;

/**
 * A transform to convert a {@link HCatRecord} from hive to {@link StructuredRecord}.
 */
public class HCatRecordTransformer {
  private final HCatSchema hCatSchema;
  private final Schema schema;

  public HCatRecordTransformer(HCatSchema hCatSchema) {
    this.hCatSchema = hCatSchema;
    this.schema = convertSchema(hCatSchema);
  }

  /**
   * <p>Converts a {@link HCatSchema} from hive to {@link Schema} for CDAP.</p>
   * <p><b>Note:</b> This conversion does not support complex types and might change the primitive type.
   * See {@link #getType(String, PrimitiveObjectInspector.PrimitiveCategory)} for details.</p>
   *
   * @param hiveSchema the {@link HCatSchema} of the hive table
   * @return {@link Schema} for the given {@link HCatSchema}
   */
  private static Schema convertSchema(HCatSchema hiveSchema) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (HCatFieldSchema field : hiveSchema.getFields()) {
      String name = field.getName();
      if (field.isComplex()) {
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with complex type %s. Only primitive types are supported.",
          name, field.getTypeString()));
      }
      fields.add(Schema.Field.of(name, Schema.of(getType(name, field.getTypeInfo().getPrimitiveCategory()))));
    }
    return Schema.recordOf("record", fields);
  }

  /**
   * Returns the {@link Schema.Type} compatible for this field from hive.
   *
   * @param name name of the field
   * @param category the field's {@link PrimitiveObjectInspector.PrimitiveCategory}
   * @return the {@link Schema.Type} for this field
   */
  private static Schema.Type getType(String name, PrimitiveObjectInspector.PrimitiveCategory category) {
    switch (category) {
      case BOOLEAN:
        return Schema.Type.BOOLEAN;
      case BYTE:
      case CHAR:
      case SHORT:
      case INT:
        return Schema.Type.INT;
      case LONG:
        return Schema.Type.LONG;
      case FLOAT:
        return Schema.Type.FLOAT;
      case DOUBLE:
        return Schema.Type.DOUBLE;
      case STRING:
      case VARCHAR:
        return Schema.Type.STRING;
      case BINARY:
        return Schema.Type.BYTES;
      case VOID:
      case DATE:
      case TIMESTAMP:
      case DECIMAL:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format("Table schema contains field '%s' with unsupported type %s",
                                                         name, category.name()));
    }
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
      switch (field.getSchema().getType()) {
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BYTES: {
          try {
            builder.set(fieldName, hCatRecord.get(fieldName, hCatSchema));
            break;
          } catch (Throwable t) {
            throw new RuntimeException(String.format("Error converting field '%s' of type %s",
                                                     fieldName, field.getSchema().getType()), t);
          }
        }
        default: {
          throw new IllegalStateException(String.format("Output schema contains field '%s' with unsupported type %s.",
                                                        fieldName, field.getSchema().getType().name()));
        }
      }
    }
    return builder.build();
  }
}
